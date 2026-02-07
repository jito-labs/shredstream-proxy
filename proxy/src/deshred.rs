use std::{
    collections::{BTreeSet, HashSet},
    hash::Hash,
    io::Cursor,
    sync::atomic::Ordering,
};

use bincode::Options as _;
use itertools::Itertools;
use jito_protos::shredstream::TraceShred;
use log::{debug, warn};
use prost::Message;
use solana_ledger::{
    blockstore::MAX_DATA_SHREDS_PER_SLOT,
    shred::{
        merkle::{Shred, ShredCode},
        ReedSolomonCache, ShredType, Shredder,
    },
};
use solana_metrics::datapoint_warn;
use solana_perf::packet::PacketBatch;
use solana_sdk::clock::{Slot, MAX_PROCESSING_AGE};

use crate::forwarder::ShredMetrics;

#[derive(Default, Debug, Copy, Clone, Eq, PartialEq)]
enum ShredStatus {
    #[default]
    Unknown,
    /// Shred that is **not** marked as [ShredFlags::DATA_COMPLETE_SHRED]
    NotDataComplete,
    /// Shred that is marked as [ShredFlags::DATA_COMPLETE_SHRED]
    DataComplete,
}

/// Tracks per-slot shred information for data shreds
/// Guaranteed to have MAX_DATA_SHREDS_PER_SLOT entries in each Vec
#[derive(Debug)]
pub struct ShredsStateTracker {
    /// Compact status of each data shred for fast iteration.
    data_status: Vec<ShredStatus>,
    /// Data shreds received for the slot (not coding!)
    data_shreds: Vec<Option<Shred>>,
    /// Indices for which a shred exists and is `DATA_COMPLETE_SHRED` (or `LAST_SHRED_IN_SLOT`).
    /// This is used to find completed deshred ranges quickly.
    data_complete_idxs: BTreeSet<usize>,
    /// array of bools that track which FEC set indexes have been already recovered
    already_recovered_fec_sets: Vec<bool>,
    /// array of bools that track which data shred indexes have been already deshredded
    already_deshredded: Vec<bool>,
}
impl Default for ShredsStateTracker {
    fn default() -> Self {
        Self {
            data_status: vec![ShredStatus::Unknown; MAX_DATA_SHREDS_PER_SLOT],
            data_shreds: vec![None; MAX_DATA_SHREDS_PER_SLOT],
            data_complete_idxs: BTreeSet::new(),
            already_recovered_fec_sets: vec![false; MAX_DATA_SHREDS_PER_SLOT],
            already_deshredded: vec![false; MAX_DATA_SHREDS_PER_SLOT],
        }
    }
}

const BINCODE_DESERIALIZE_LIMIT_BYTES: u64 = 64 * 1024 * 1024;

/// Returns the number of **data** shreds reconstructed via FEC recovery.
/// Updates all_shreds with current state, and deshredded_entries with returned values
/// receive shreds per FEC set, attempting to recover the other shreds in the fec set so you do not have to wait until all data shreds have arrived.
/// every time a fec is recovered, scan for neighbouring DATA_COMPLETE_SHRED flags in the shreds, attempting to deserialize into solana entries when there are no missing shreds between the DATA_COMPLETE_SHRED flags.
/// note that an FEC set doesn't necessarily contain DATA_COMPLETE_SHRED in the last shred. when deserializing the bincode data, you must use data between shreds starting at the last DATA_COMPLETE_SHRED (not inclusive) to the next DATA_COMPLETE_SHRED (inclusive)
pub fn reconstruct_shreds(
    packet_batch: PacketBatch,
    all_shreds: &mut ahash::HashMap<
        Slot,
        (
            ahash::HashMap<u32 /* fec_set_index */, HashSet<ComparableShred>>,
            ShredsStateTracker,
        ),
    >,
    slot_fec_indexes_to_iterate: &mut Vec<(Slot, u32)>,
    deshredded_entries: &mut Vec<(Slot, Vec<solana_entry::entry::Entry>, Vec<u8>)>,
    highest_slot_seen: &mut Slot,
    rs_cache: &ReedSolomonCache,
    metrics: &ShredMetrics,
) -> usize {
    deshredded_entries.clear();
    slot_fec_indexes_to_iterate.clear();
    // ingest all packets
    for packet in packet_batch.iter().filter_map(|p| p.data(..)) {
        match solana_ledger::shred::Shred::new_from_serialized_shred(packet.to_vec())
            .and_then(Shred::try_from)
        {
            Ok(shred) => {
                let slot = shred.common_header().slot;
                let index = shred.index() as usize;
                let fec_set_index = shred.fec_set_index();
                if index >= MAX_DATA_SHREDS_PER_SLOT
                    || (fec_set_index as usize) >= MAX_DATA_SHREDS_PER_SLOT
                {
                    debug!(
                        "Out-of-bounds shred slot: {slot}, fec_set_index: {fec_set_index}, index: {index}"
                    );
                    continue;
                }
                let (all_shreds, state_tracker) = all_shreds.entry(slot).or_default();
                if highest_slot_seen.saturating_sub(SLOT_LOOKBACK) > slot {
                    debug!(
                        "Old shred slot: {slot}, fec_set_index: {fec_set_index}, index: {index}"
                    );
                    continue;
                }
                if state_tracker.already_recovered_fec_sets[fec_set_index as usize]
                    || state_tracker.already_deshredded[index]
                {
                    debug!("Already completed slot: {slot}, fec_set_index: {fec_set_index}, index: {index}");
                    continue;
                }
                let Some(_shred_index) = update_state_tracker(&shred, state_tracker) else {
                    continue;
                };

                all_shreds
                    .entry(fec_set_index)
                    .or_default()
                    .insert(ComparableShred(shred));
                slot_fec_indexes_to_iterate.push((slot, fec_set_index)); // use Vec so we can sort to make sure if any earlier FEC sets have DATA_SHRED_COMPLETE, later entries can use the flag to find the bounds
                *highest_slot_seen = std::cmp::max(*highest_slot_seen, slot);
            }
            Err(e) => {
                if TraceShred::decode(packet).is_ok() {
                    continue;
                }
                warn!("Failed to decode shred. Err: {e:?}");
            }
        }
    }
    slot_fec_indexes_to_iterate.sort_unstable();
    slot_fec_indexes_to_iterate.dedup();

    // Try recovering by FEC set.
    // Note: `merkle::recover` can return both recovered data and coding shreds; we only
    // care about recovered *data* shreds for decoding entries.
    let mut total_recovered_data_shreds = 0usize;
    for (slot, fec_set_index) in slot_fec_indexes_to_iterate.iter() {
        let (all_shreds, state_tracker) = all_shreds.entry(*slot).or_default();
        let shreds = all_shreds.entry(*fec_set_index).or_default();
        let (
            num_expected_data_shreds,
            num_expected_coding_shreds,
            num_data_shreds,
            num_coding_shreds,
        ) = get_data_shred_info(shreds);

        // If we already have all expected data shreds for this FEC set, mark it complete and
        // drop stored shards to cap memory.
        if num_expected_data_shreds > 0
            && has_all_data_shreds_for_fec_set(
                state_tracker,
                *fec_set_index,
                num_expected_data_shreds,
            )
        {
            state_tracker.already_recovered_fec_sets[*fec_set_index as usize] = true;
            shreds.clear();
            continue;
        }

        // Haven't received enough shards yet, or don't have coding shreds to recover with.
        // `merkle::recover` requires at least one coding shred.
        let min_shreds_needed_to_recover = num_expected_data_shreds as usize;
        if num_expected_data_shreds == 0
            || num_coding_shreds == 0
            || shreds.len() < min_shreds_needed_to_recover
        {
            continue;
        }

        // try to recover if we have enough shreds in the FEC set
        // `merkle::recover` internally sorts shreds by erasure shard index, so avoid doing it twice.
        let merkle_shreds = shreds.iter().map(|s| s.0.clone()).collect_vec();
        let recovered = match solana_ledger::shred::merkle::recover(merkle_shreds, rs_cache) {
            Ok(r) => r, // data shreds followed by code shreds (whatever was missing from to_deshred_payload)
            Err(e) => {
                warn!(
                    "Failed to recover shreds for slot {slot} fec_set_index {fec_set_index}. num_expected_data_shreds: {num_expected_data_shreds}, num_data_shreds: {num_data_shreds} num_expected_coding_shreds: {num_expected_coding_shreds} num_coding_shreds: {num_coding_shreds} Err: {e}",
                );
                continue;
            }
        };

        let mut fec_set_recovered_data_shreds = 0usize;
        for shred in recovered {
            match shred {
                Ok(shred) => {
                    let is_data = shred.shred_type() == ShredType::Data;
                    if update_state_tracker(&shred, state_tracker).is_none() {
                        continue; // already seen before in state tracker
                    }
                    if is_data {
                        // shreds.insert(ComparableShred(shred)); // optional since all data shreds are in state_tracker
                        total_recovered_data_shreds += 1;
                        fec_set_recovered_data_shreds += 1;
                    }
                }
                Err(e) => warn!(
                    "Failed to recover shred for slot {slot}, fec set: {fec_set_index}. Err: {e}"
                ),
            }
        }

        if fec_set_recovered_data_shreds > 0 {
            debug!(
                "recovered slot: {slot}, fec_index: {fec_set_index}, recovered data count: {fec_set_recovered_data_shreds}"
            );
        }

        // If recovery completed the data-shred set for this erasure batch, mark as complete and
        // drop stored shards. Otherwise, keep them so we can retry as new shreds arrive.
        if num_expected_data_shreds > 0
            && has_all_data_shreds_for_fec_set(
                state_tracker,
                *fec_set_index,
                num_expected_data_shreds,
            )
        {
            state_tracker.already_recovered_fec_sets[*fec_set_index as usize] = true;
            shreds.clear();
        }
    }

    // deshred and bincode deserialize
    for slot in slot_fec_indexes_to_iterate
        .iter()
        .map(|(slot, _fec_set_index)| *slot)
        .dedup()
    {
        let (_all_shreds, state_tracker) = all_shreds.entry(slot).or_default();

        // Snapshot so we can mutate tracker while iterating.
        let data_complete_idxs = state_tracker
            .data_complete_idxs
            .iter()
            .copied()
            .collect_vec();
        for end_data_complete_idx in data_complete_idxs {
            if end_data_complete_idx >= state_tracker.data_status.len() {
                continue;
            }
            if state_tracker.already_deshredded[end_data_complete_idx] {
                continue;
            }
            if !matches!(
                state_tracker.data_status[end_data_complete_idx],
                ShredStatus::DataComplete
            ) {
                continue;
            }

            // Find start boundary:
            // - If we have a previous DATA_COMPLETE_SHRED, start right after it.
            // - Otherwise, start at 0.
            // - If there's a gap (missing shreds), allow an "unknown start" at the first present
            //   shred after the gap (best-effort).
            let (start_data_complete_idx, unknown_start) = {
                let mut start = end_data_complete_idx;
                let mut unknown = false;
                let mut i = end_data_complete_idx;
                while i > 0 {
                    let prev = i - 1;
                    if matches!(state_tracker.data_status[prev], ShredStatus::DataComplete) {
                        start = i;
                        break;
                    }
                    // Treat missing shreds as a boundary only if they haven't already been
                    // consumed (we may have dropped payloads for already-deshredded indices).
                    if state_tracker.data_shreds[prev].is_none()
                        && !state_tracker.already_deshredded[prev]
                    {
                        start = i;
                        unknown = true;
                        break;
                    }
                    i = prev;
                }
                if i == 0 {
                    start = 0;
                }
                (start, unknown)
            };

            if unknown_start {
                metrics
                    .unknown_start_position_count
                    .fetch_add(1, Ordering::Relaxed);
            }

            // For best-effort decoding when we don't know the true start boundary, require that the
            // chosen start index is at the beginning of an erasure batch. This reduces accidental
            // decodes when there are gaps within a data set.
            if unknown_start {
                let Some(start_shred) = state_tracker.data_shreds[start_data_complete_idx].as_ref()
                else {
                    continue;
                };
                if start_shred.fec_set_index() as usize != start_data_complete_idx {
                    continue;
                }
            }

            // Require all shreds in the candidate range.
            if (start_data_complete_idx..=end_data_complete_idx).any(|idx| {
                state_tracker.already_deshredded[idx] || state_tracker.data_shreds[idx].is_none()
            }) {
                continue;
            }

            let to_deshred =
                &state_tracker.data_shreds[start_data_complete_idx..=end_data_complete_idx];
            let deshredded_payload = match Shredder::deshred(
                to_deshred.iter().map(|s| s.as_ref().unwrap().payload()),
            ) {
                Ok(v) => v,
                Err(e) => {
                    warn!("slot {slot} failed to deshred slot: {slot}, start_data_complete_idx: {start_data_complete_idx}, end_data_complete_idx: {end_data_complete_idx}. Err: {e}");
                    metrics
                        .fec_recovery_error_count
                        .fetch_add(1, Ordering::Relaxed);
                    if unknown_start {
                        metrics
                            .unknown_start_position_error_count
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    continue;
                }
            };

            let bincode_opts = bincode::DefaultOptions::new()
                .with_fixint_encoding()
                .with_limit(BINCODE_DESERIALIZE_LIMIT_BYTES);
            let mut cursor = Cursor::new(&deshredded_payload);
            let entries = match bincode_opts
                .allow_trailing_bytes()
                .deserialize_from::<_, Vec<solana_entry::entry::Entry>>(&mut cursor)
            {
                Ok(entries) => {
                    let consumed = cursor.position() as usize;
                    let trailing = &deshredded_payload[consumed..];
                    if !trailing.is_empty() {
                        // The Solana deshredder can emit a buffer with extra trailing bytes for
                        // empty entry vectors (backward compat), and some producers may include
                        // zero-padding in the last shred's `size`. Accept trailing zeros; reject
                        // non-zero trailing bytes as likely corruption or a boundary mismatch.
                        let trailing_all_zeros = trailing.iter().all(|&b| b == 0);
                        if !trailing_all_zeros {
                            debug!(
                                "Rejecting deserialized entries with trailing bytes for slot {slot}, start_data_complete_idx: {start_data_complete_idx}, end_data_complete_idx: {end_data_complete_idx}, unknown_start: {unknown_start}. trailing_len: {}",
                                trailing.len(),
                            );
                            metrics
                                .bincode_deserialize_error_count
                                .fetch_add(1, Ordering::Relaxed);
                            if unknown_start {
                                metrics
                                    .unknown_start_position_error_count
                                    .fetch_add(1, Ordering::Relaxed);
                            }
                            continue;
                        }
                    }
                    entries
                }
                Err(e) => {
                    debug!(
                        "Failed to deserialize bincode payload of size {} for slot {slot}, start_data_complete_idx: {start_data_complete_idx}, end_data_complete_idx: {end_data_complete_idx}, unknown_start: {unknown_start}. Err: {e}",
                        deshredded_payload.len()
                    );
                    metrics
                        .bincode_deserialize_error_count
                        .fetch_add(1, Ordering::Relaxed);
                    if unknown_start {
                        metrics
                            .unknown_start_position_error_count
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    continue;
                }
            };

            metrics
                .entry_count
                .fetch_add(entries.len() as u64, Ordering::Relaxed);
            let txn_count = entries.iter().map(|e| e.transactions.len() as u64).sum();
            metrics.txn_count.fetch_add(txn_count, Ordering::Relaxed);
            debug!(
                "Successfully decoded slot: {slot} start_data_complete_idx: {start_data_complete_idx} end_data_complete_idx: {end_data_complete_idx} with entry count: {}, txn count: {txn_count}",
                entries.len(),
            );

            deshredded_entries.push((slot, entries, deshredded_payload));

            // Mark as consumed and drop payloads to cap memory.
            for idx in start_data_complete_idx..=end_data_complete_idx {
                state_tracker.already_deshredded[idx] = true;
                state_tracker.data_shreds[idx] = None;
            }
        }
    }

    if all_shreds.len() > MAX_PROCESSING_AGE {
        let slot_threshold = highest_slot_seen.saturating_sub(SLOT_LOOKBACK);
        let mut incomplete_fec_sets = ahash::HashMap::<Slot, Vec<_>>::default();
        let mut incomplete_fec_sets_count = 0;
        all_shreds.retain(|slot, (fec_set_indexes, state_tracker)| {
            if *slot >= slot_threshold {
                return true;
            }

            // count missing fec sets before clearing
            for (fec_set_index, shreds) in fec_set_indexes.iter() {
                if state_tracker.already_recovered_fec_sets[*fec_set_index as usize] {
                    continue;
                }
                let (
                    num_expected_data_shreds,
                    _num_expected_coding_shreds,
                    _num_data_shreds,
                    _num_coding_shreds,
                ) = get_data_shred_info(shreds);

                incomplete_fec_sets_count += 1;
                incomplete_fec_sets
                    .entry(*slot)
                    .and_modify(|fec_set_data| {
                        fec_set_data.push((*fec_set_index, num_expected_data_shreds, shreds.len()))
                    })
                    .or_insert_with(|| {
                        vec![(*fec_set_index, num_expected_data_shreds, shreds.len())]
                    });
            }

            false
        });
        if incomplete_fec_sets_count > 0 {
            incomplete_fec_sets
                .iter_mut()
                .for_each(|(_slot, fec_set_indexes)| fec_set_indexes.sort_unstable());
            datapoint_warn!(
                "shredstream_proxy-deshred_missed_fec_sets",
                (
                    "slot_fec_set_indexes",
                    format!("{:?}", incomplete_fec_sets.iter().sorted().collect_vec()),
                    String
                ),
                ("slot_count", incomplete_fec_sets.len(), i64),
                ("fec_set_count", incomplete_fec_sets_count, i64),
            );
        }
    }

    if total_recovered_data_shreds > 0 {
        metrics
            .recovered_count
            .fetch_add(total_recovered_data_shreds as u64, Ordering::Relaxed);
    }

    total_recovered_data_shreds
}

#[allow(unused)]
fn debug_remaining_shreds(
    all_shreds: &mut ahash::HashMap<
        Slot,
        (
            ahash::HashMap<u32, HashSet<ComparableShred>>,
            ShredsStateTracker,
        ),
    >,
) {
    let mut incomplete_fec_sets = ahash::HashMap::<Slot, Vec<_>>::default();
    let mut incomplete_fec_sets_count = 0;
    all_shreds
        .iter()
        .for_each(|(slot, (fec_set_indexes, state_tracker))| {
            // count missing fec sets before clearing
            for (fec_set_index, shreds) in fec_set_indexes.iter() {
                if state_tracker.already_recovered_fec_sets[*fec_set_index as usize] {
                    continue;
                }
                let (
                    num_expected_data_shreds,
                    _num_expected_coding_shreds,
                    _num_data_shreds,
                    _num_coding_shreds,
                ) = get_data_shred_info(shreds);

                incomplete_fec_sets_count += 1;
                incomplete_fec_sets
                    .entry(*slot)
                    .and_modify(|fec_set_data| {
                        fec_set_data.push((*fec_set_index, num_expected_data_shreds, shreds.len()))
                    })
                    .or_insert_with(|| {
                        vec![(*fec_set_index, num_expected_data_shreds, shreds.len())]
                    });
            }
        });
    incomplete_fec_sets
        .iter_mut()
        .for_each(|(_slot, fec_set_indexes)| fec_set_indexes.sort_unstable());
    println!("{:?}", incomplete_fec_sets.iter().sorted().collect_vec());
}

/// Return the inclusive range of shreds that constitute one complete segment: [0+ NotDataComplete, DataComplete]
/// Rules:
/// * A segment **ends** at the first `DataComplete` *at or after* `index`.
/// * It **starts** one position after the previous `DataComplete`, or at the beginning of the vector if there is none.
/// * If an `Unknown` is seen while searching towards the right, the segment is discarded and `None` is returned.
/// * We allow `Unknown` towards the left since sometimes entire FEC sets are not sent out
fn get_indexes(
    tracker: &ShredsStateTracker,
    index: usize,
) -> Option<(
    usize, /* start_data_complete_idx */
    usize, /* end_data_complete_idx */
    bool,  /* unknown start index */
)> {
    if index >= tracker.data_status.len() {
        return None;
    }

    // find the right boundary (first DataComplete ≥ index)
    let mut end = index;
    while end < tracker.data_status.len() {
        if tracker.already_deshredded[end] {
            return None;
        }
        match &tracker.data_status[end] {
            ShredStatus::Unknown => return None,
            ShredStatus::DataComplete => break,
            ShredStatus::NotDataComplete => end += 1,
        }
    }
    if end == tracker.data_status.len() {
        return None; // never saw a DataComplete
    }

    if end == 0 {
        return Some((0, 0, false)); // the vec *starts* with DataComplete
    }
    if index == 0 {
        return Some((0, end, false));
    }

    // find the left boundary (prev DataComplete + 1)
    let mut start = index;
    let mut next = start - 1;
    loop {
        match tracker.data_status[next] {
            ShredStatus::NotDataComplete => {
                if tracker.already_deshredded[next] {
                    return None; // already covered by some other iteration
                }
                if next == 0 {
                    return Some((0, end, false)); // no earlier DataComplete
                }
                start = next;
                next -= 1;
            }
            ShredStatus::DataComplete => return Some((start, end, false)),
            ShredStatus::Unknown => return Some((start, end, true)), // sometimes we don't have the previous starting shreds, make best guess
        }
    }
}

/// Upon receiving a new shred (either from recovery or receiving a UDP packet), update the state tracker
/// Returns shred index on new insert, None if already exists
fn update_state_tracker(shred: &Shred, state_tracker: &mut ShredsStateTracker) -> Option<usize> {
    let index = shred.index() as usize;
    let fec_set_index = shred.fec_set_index() as usize;
    if index >= state_tracker.data_shreds.len()
        || fec_set_index >= state_tracker.already_recovered_fec_sets.len()
    {
        return None;
    }
    if state_tracker.already_recovered_fec_sets[fec_set_index] {
        return None;
    }
    if shred.shred_type() == ShredType::Data
        && (state_tracker.data_shreds[index].is_some()
            || !matches!(state_tracker.data_status[index], ShredStatus::Unknown))
    {
        return None;
    }
    if let Shred::ShredData(s) = &shred {
        state_tracker.data_shreds[index] = Some(shred.clone());
        if s.data_complete() || s.last_in_slot() {
            state_tracker.data_status[index] = ShredStatus::DataComplete;
            state_tracker.data_complete_idxs.insert(index);
        } else {
            state_tracker.data_status[index] = ShredStatus::NotDataComplete;
        }
    };
    Some(index)
}

const SLOT_LOOKBACK: Slot = 50;

fn has_all_data_shreds_for_fec_set(
    tracker: &ShredsStateTracker,
    fec_set_index: u32,
    num_expected_data_shreds: u16,
) -> bool {
    let start = fec_set_index as usize;
    let Some(end) = start.checked_add(num_expected_data_shreds as usize).and_then(|v| v.checked_sub(1)) else {
        return false;
    };
    if end >= tracker.data_shreds.len() {
        return false;
    }

    (start..=end).all(|idx| tracker.data_shreds[idx].is_some() || tracker.already_deshredded[idx])
}

/// check if we can reconstruct (having minimum number of data + coding shreds)
fn get_data_shred_info(
    shreds: &HashSet<ComparableShred>,
) -> (
    u16, /* num_expected_data_shreds */
    u16, /* num_expected_coding_shreds */
    u16, /* num_data_shreds */
    u16, /* num_coding_shreds */
) {
    let mut num_expected_data_shreds = 0;
    let mut num_expected_coding_shreds = 0;
    let mut num_data_shreds = 0;
    let mut num_coding_shreds = 0;
    for shred in shreds {
        match &shred.0 {
            Shred::ShredCode(s) => {
                num_coding_shreds += 1;
                num_expected_data_shreds = s.coding_header.num_data_shreds;
                num_expected_coding_shreds = s.coding_header.num_coding_shreds;
            }
            Shred::ShredData(s) => {
                num_data_shreds += 1;
                if num_expected_data_shreds == 0 && (s.data_complete() || s.last_in_slot()) {
                    num_expected_data_shreds =
                        (shred.0.index() - shred.0.fec_set_index()) as u16 + 1;
                }
            }
        }
    }
    (
        num_expected_data_shreds,
        num_expected_coding_shreds,
        num_data_shreds,
        num_coding_shreds,
    )
}

/// Issue: datashred equality comparison is wrong due to data size being smaller than the 1203 bytes allocated
#[derive(Clone, Debug, Eq)]
pub struct ComparableShred(Shred);

impl std::ops::Deref for ComparableShred {
    type Target = Shred;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Hash for ComparableShred {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match &self.0 {
            Shred::ShredCode(s) => {
                s.common_header.hash(state);
                s.coding_header.hash(state);
            }
            Shred::ShredData(s) => {
                s.common_header.hash(state);
                s.data_header.hash(state);
            }
        }
    }
}

impl PartialEq for ComparableShred {
    // Custom comparison to avoid random bytes that are part of payload
    fn eq(&self, other: &Self) -> bool {
        match &self.0 {
            Shred::ShredCode(s1) => match &other.0 {
                Shred::ShredCode(s2) => {
                    let solana_ledger::shred::ShredVariant::MerkleCode {
                        proof_size,
                        chained: _,
                        resigned,
                    } = s1.common_header.shred_variant
                    else {
                        return false;
                    };

                    // see https://github.com/jito-foundation/jito-solana/blob/d6c73374e3b4f863436e4b7d4d1ce5eea01cd262/ledger/src/shred/merkle.rs#L346, and re-add the proof component
                    let comparison_len =
                        <ShredCode as solana_ledger::shred::traits::Shred>::SIZE_OF_PAYLOAD
                            .saturating_sub(
                                usize::from(proof_size)
                                    * solana_ledger::shred::merkle::SIZE_OF_MERKLE_PROOF_ENTRY
                                    + if resigned {
                                        solana_ledger::shred::SIZE_OF_SIGNATURE
                                    } else {
                                        0
                                    },
                            );

                    s1.coding_header == s2.coding_header
                        && s1.common_header == s2.common_header
                        && s1.payload[..comparison_len] == s2.payload[..comparison_len]
                }
                Shred::ShredData(_) => false,
            },
            Shred::ShredData(s1) => match &other.0 {
                Shred::ShredCode(_) => false,
                Shred::ShredData(s2) => {
                    let Ok(s1_data) = solana_ledger::shred::layout::get_data(self.payload()) else {
                        return false;
                    };
                    let Ok(s2_data) = solana_ledger::shred::layout::get_data(other.payload())
                    else {
                        return false;
                    };
                    s1.data_header == s2.data_header
                        && s1.common_header == s2.common_header
                        && s1_data == s2_data
                }
            },
        }
    }
}
#[cfg(test)]
mod tests {
    use std::{
        collections::{hash_map::Entry, HashSet},
        io::{Read, Write},
        net::UdpSocket,
        sync::Arc,
    };

    use borsh::BorshDeserialize;
    use itertools::Itertools;
    use rand::Rng;
    use solana_ledger::{
        blockstore::make_slot_entries_with_transactions,
        shred::{merkle::Shred, ProcessShredsStats, ReedSolomonCache, ShredCommonHeader, Shredder},
    };
    use solana_perf::packet::{Packet, PacketBatch};
    use solana_sdk::{clock::Slot, hash::Hash, signature::Keypair};

    use crate::{
        deshred::{reconstruct_shreds, ComparableShred},
        forwarder::ShredMetrics,
    };

    /// For serializing packets to disk
    #[derive(borsh::BorshSerialize, borsh::BorshDeserialize, PartialEq, Debug)]
    struct Packets {
        pub packets: Vec<Vec<u8>>,
    }

    #[allow(unused)]
    fn listen_and_write_shreds() -> std::io::Result<()> {
        let socket = UdpSocket::bind("127.0.0.1:5000")?;
        println!("Listening on {}", socket.local_addr()?);

        let mut map = ahash::HashMap::<usize, usize>::default();
        let mut buf = [0u8; 1500];
        let mut vec = Packets {
            packets: Vec::new(),
        };

        let mut i = 0;
        loop {
            i += 1;
            match socket.recv_from(&mut buf) {
                Ok((amt, _src)) => {
                    vec.packets.push(buf[..amt].to_vec());
                    match map.entry(amt) {
                        Entry::Occupied(mut e) => *e.get_mut() += 1,
                        Entry::Vacant(e) => {
                            e.insert(1);
                        }
                    }
                    *map.get_mut(&amt).unwrap_or(&mut 0) += 1;
                }
                Err(e) => {
                    eprintln!("Error receiving data: {}", e);
                }
            }
            if i % 50000 == 0 {
                dbg!(&map);
                // size 1203 are data shreds: https://github.com/jito-foundation/jito-solana/blob/1742826fca975bd6d17daa5693abda861bbd2adf/ledger/src/shred/merkle.rs#L42
                // size 1228 are coding shreds: https://github.com/jito-foundation/jito-solana/blob/1742826fca975bd6d17daa5693abda861bbd2adf/ledger/src/shred/shred_code.rs#L16
                let mut file = std::fs::File::create("serialized_shreds.bin")?;
                file.write_all(&borsh::to_vec(&vec)?)?;
                return Ok(());
            }
        }
    }

    #[test]
    fn test_reconstruct_live_shreds() {
        let packets = {
            let mut file = std::fs::File::open("../bins/serialized_shreds.bin").unwrap();
            let mut buffer = Vec::new();
            file.read_to_end(&mut buffer).unwrap();
            Packets::try_from_slice(&buffer).unwrap()
        };
        assert_eq!(packets.packets.len(), 50_000);

        let shreds = packets
            .packets
            .iter()
            .filter_map(|p| Shred::from_payload(p.clone()).ok())
            .collect::<Vec<_>>();
        assert_eq!(shreds.len(), 49989);

        let unique_shreds = packets
            .packets
            .iter()
            .filter_map(|p| Shred::from_payload(p.clone()).ok().map(ComparableShred))
            .collect::<HashSet<ComparableShred>>();
        assert_eq!(unique_shreds.len(), 44900);

        let unique_slot_fec_shreds = packets
            .packets
            .iter()
            .filter_map(|p| {
                Shred::from_payload(p.clone())
                    .ok()
                    .map(|s| *s.common_header())
            })
            .collect::<HashSet<ShredCommonHeader>>();
        assert_eq!(unique_slot_fec_shreds.len(), 44900);

        let rs_cache = ReedSolomonCache::default();
        let metrics = Arc::new(ShredMetrics::default());

        // Test 1: all shreds provided
        let mut all_shreds = ahash::HashMap::default();
        let mut slot_fec_indexes_to_iterate: Vec<(Slot, u32)> = Vec::new();
        let mut deshredded_entries = Vec::new();
        let mut highest_slot_seen = 0;
        let recovered_count = reconstruct_shreds(
            PacketBatch::new(
                packets
                    .packets
                    .iter()
                    .map(|x| {
                        let mut packet = Packet::default();
                        packet.buffer_mut()[..x.len()].copy_from_slice(x);
                        packet.meta_mut().size = x.len();
                        packet
                    })
                    .collect_vec(),
            ),
            &mut all_shreds,
            &mut slot_fec_indexes_to_iterate,
            &mut deshredded_entries,
            &mut highest_slot_seen,
            &rs_cache,
            &metrics,
        );

        // debug_to_disk(&mut deshredded_entries);
        assert!(recovered_count < deshredded_entries.len());
        assert_decoded_entries_sane(&deshredded_entries);
        let total_entries = deshredded_entries
            .iter()
            .map(|(_slot, entries, _entries_bytes)| entries.len())
            .sum::<usize>();
        assert!(total_entries >= 13580, "total_entries: {total_entries}");
        assert_eq!(all_shreds.len(), 30);

        let slot_to_entry = deshredded_entries
            .iter()
            .into_group_map_by(|(slot, _entries, _entries_bytes)| *slot);
        // slot_to_entry
        //     .iter()
        //     .sorted_by_key(|(slot, _)| *slot)
        //     .for_each(|(slot, entry)| {
        //         println!(
        //             "slot {slot} entry count: {:?}, txn count: {}",
        //             entry.len(),
        //             entry
        //                 .iter()
        //                 .map(|(_slot, entry)| entry.transactions.len())
        //                 .sum::<usize>()
        //         );
        //     });
        assert!(
            slot_to_entry.len() >= 29,
            "slot_to_entry.len(): {}",
            slot_to_entry.len()
        );

        // Test 2: 33% of shreds missing
        let mut all_shreds = ahash::HashMap::default();
        let mut slot_fec_indexes_to_iterate: Vec<(Slot, u32)> = Vec::new();
        let mut deshredded_entries = Vec::new();
        let mut highest_slot_seen = 0;
        let recovered_count = reconstruct_shreds(
            PacketBatch::new(
                packets
                    .packets
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| (index + 1) % 3 != 0)
                    .map(|(_i, x)| {
                        let mut packet = Packet::default();
                        packet.buffer_mut()[..x.len()].copy_from_slice(x);
                        packet.meta_mut().size = x.len();
                        packet
                    })
                    .collect_vec(),
            ),
            &mut all_shreds,
            &mut slot_fec_indexes_to_iterate,
            &mut deshredded_entries,
            &mut highest_slot_seen,
            &rs_cache,
            &metrics,
        );

        // debug_to_disk(&deshredded_entries, "new.txt");
        assert!(recovered_count > 0);
        assert_decoded_entries_sane(&deshredded_entries);
        let total_entries = deshredded_entries
            .iter()
            .map(|(_slot, entries, _entries_bytes)| entries.len())
            .sum::<usize>();
        assert!(total_entries >= 13580, "total_entries: {total_entries}");
        assert!(all_shreds.len() > 15);

        let slot_to_entry = deshredded_entries
            .iter()
            .into_group_map_by(|(slot, _entries, _entries_bytes)| *slot);
        assert!(
            slot_to_entry.len() >= 29,
            "slot_to_entry.len(): {}",
            slot_to_entry.len()
        );
    }

    /// Helper function to compare all shred output
    #[allow(unused)]
    fn debug_to_disk(
        deshredded_entries: &[(Slot, Vec<solana_entry::entry::Entry>, Vec<u8>)],
        filepath: &str,
    ) {
        let entries = deshredded_entries
            .iter()
            .map(|(slot, entries, _entries_bytes)| (slot, entries))
            .into_group_map_by(|(slot, _entries)| *slot)
            .into_iter()
            .map(|(key, values)| {
                (
                    key,
                    values.into_iter().fold(Vec::new(), |mut acc, (_, v)| {
                        acc.extend(v);
                        acc
                    }),
                )
            })
            .map(|(slot, entries)| {
                let mut vec = entries
                    .iter()
                    .flat_map(|x| x.transactions.iter())
                    .map(|x| x.signatures[0])
                    .collect::<Vec<_>>();
                vec.sort();
                vec.dedup();
                (slot, vec)
            })
            .sorted_by_key(|x| x.0)
            .dedup_by(|lhs, rhs| lhs.0 == rhs.0)
            .collect_vec();
        let mut file = std::fs::File::create(filepath).unwrap();
        write!(file, "entries: {:#?}", &entries).unwrap();
    }

    fn assert_decoded_entries_sane(
        deshredded_entries: &[(Slot, Vec<solana_entry::entry::Entry>, Vec<u8>)],
    ) {
        // Keep these checks cheap: the fixture tests already do a lot of work.
        const MAX_HASH_CHAIN_VECS: usize = 64;

        let mut seen_by_slot =
            ahash::HashMap::<Slot, HashSet<solana_sdk::signature::Signature>>::default();
        let mut hash_chain_vecs_checked = 0usize;

        for (slot, entries, _entries_bytes) in deshredded_entries {
            // Basic structural validation: do a small amount of PoH hash-chain checking, but cap
            // the total work so fixture tests remain fast.
            if hash_chain_vecs_checked < MAX_HASH_CHAIN_VECS && entries.len() >= 2 {
                // Prefix transition.
                assert!(
                    entries[1].verify(&entries[0].hash),
                    "slot {slot} contains an invalid entry hash chain (prefix)"
                );
                // Middle transition.
                let mid = entries.len() / 2;
                if mid > 0 {
                    assert!(
                        entries[mid].verify(&entries[mid - 1].hash),
                        "slot {slot} contains an invalid entry hash chain (mid)"
                    );
                }
                // Suffix transition.
                let last = entries.len() - 1;
                assert!(
                    entries[last].verify(&entries[last - 1].hash),
                    "slot {slot} contains an invalid entry hash chain (suffix)"
                );
                hash_chain_vecs_checked += 1;
            }

            // Ensure we never emit duplicate transactions within the same slot.
            let seen = seen_by_slot.entry(*slot).or_default();
            for entry in entries {
                for tx in &entry.transactions {
                    let sig = tx
                        .signatures
                        .get(0)
                        .copied()
                        .expect("transaction missing signature");
                    assert!(
                        seen.insert(sig),
                        "slot {slot} contains duplicate tx signature: {sig:?}",
                    );
                }
            }
        }
    }

    #[test]
    fn test_recovered_count_counts_only_data_shreds() {
        let slot = 222_222;
        let leader_keypair = Arc::new(Keypair::new());
        let reed_solomon_cache = ReedSolomonCache::default();
        let shredder = Shredder::new(slot, slot - 1, 0, 0).unwrap();

        // One entry group => one FEC set (32 data + 32 coding in fixed-FEC mode).
        let entries = make_slot_entries_with_transactions(64);
        let (data_shreds, coding_shreds) = shredder.entries_to_shreds(
            &leader_keypair,
            entries.as_slice(),
            true,
            None, // chained_merkle_root
            0,    // next_shred_index
            0,    // next_code_index
            true, // merkle_variant
            &reed_solomon_cache,
            &mut ProcessShredsStats::default(),
        );
        assert_eq!(data_shreds.len(), 32);
        assert_eq!(coding_shreds.len(), 32);

        // Drop some data and some coding shreds, but keep >= 32 total shards so recovery is
        // possible. We expect the recovered_count to include only the missing *data* shreds.
        const DROP_DATA: usize = 5;
        const DROP_CODE: usize = 7;
        let missing_data_indices = (0..DROP_DATA).collect::<HashSet<_>>();
        let missing_code_indices = (0..DROP_CODE).collect::<HashSet<_>>();

        let packets = data_shreds
            .iter()
            .enumerate()
            .filter(|(i, _)| !missing_data_indices.contains(i))
            .map(|(_, s)| s)
            .chain(
                coding_shreds
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| !missing_code_indices.contains(i))
                    .map(|(_, s)| s),
            )
            .map(|s| {
                let mut p = Packet::default();
                s.copy_to_packet(&mut p);
                p
            })
            .collect_vec();
        assert!(packets.len() >= 32);

        let metrics = Arc::new(ShredMetrics::default());
        let rs_cache = ReedSolomonCache::default();

        let mut all_shreds = ahash::HashMap::default();
        let mut slot_fec_indexes_to_iterate: Vec<(Slot, u32)> = Vec::new();
        let mut deshredded_entries = Vec::new();
        let mut highest_slot_seen = 0;
        let recovered_count = reconstruct_shreds(
            PacketBatch::new(packets),
            &mut all_shreds,
            &mut slot_fec_indexes_to_iterate,
            &mut deshredded_entries,
            &mut highest_slot_seen,
            &rs_cache,
            &metrics,
        );

        assert_eq!(
            recovered_count, DROP_DATA,
            "expected to count only recovered data shreds"
        );
        // We should still be able to decode the entries.
        assert_eq!(
            deshredded_entries
                .iter()
                .map(|(_slot, entries, _entries_bytes)| entries.len())
                .sum::<usize>(),
            entries.len()
        );
    }

    #[test]
    fn test_marks_fec_set_complete_without_recovery() {
        let slot = 333_333;
        let leader_keypair = Arc::new(Keypair::new());
        let reed_solomon_cache = ReedSolomonCache::default();
        let shredder = Shredder::new(slot, slot - 1, 0, 0).unwrap();

        let entries = make_slot_entries_with_transactions(64);
        let (data_shreds, coding_shreds) = shredder.entries_to_shreds(
            &leader_keypair,
            entries.as_slice(),
            true,
            None, // chained_merkle_root
            0,    // next_shred_index
            0,    // next_code_index
            true, // merkle_variant
            &reed_solomon_cache,
            &mut ProcessShredsStats::default(),
        );
        assert_eq!(data_shreds.len(), 32);
        assert_eq!(coding_shreds.len(), 32);

        let packets = data_shreds
            .iter()
            .chain(coding_shreds.iter())
            .map(|s| {
                let mut p = Packet::default();
                s.copy_to_packet(&mut p);
                p
            })
            .collect_vec();

        let metrics = Arc::new(ShredMetrics::default());
        let rs_cache = ReedSolomonCache::default();

        let mut all_shreds = ahash::HashMap::default();
        let mut slot_fec_indexes_to_iterate: Vec<(Slot, u32)> = Vec::new();
        let mut deshredded_entries = Vec::new();
        let mut highest_slot_seen = 0;
        let recovered_count = reconstruct_shreds(
            PacketBatch::new(packets),
            &mut all_shreds,
            &mut slot_fec_indexes_to_iterate,
            &mut deshredded_entries,
            &mut highest_slot_seen,
            &rs_cache,
            &metrics,
        );

        assert_eq!(recovered_count, 0);
        let (_fec_sets, tracker) = all_shreds.get(&slot).expect("slot entry should exist");
        assert!(
            tracker.already_recovered_fec_sets[0],
            "expected fec_set_index=0 to be marked complete once all data shreds are present"
        );
    }

    #[test]
    /// Test if DATA_COMPLETE_SHRED across multiple FEC sets is handled correctly
    fn test_reconstruct_live_data_complete_shred() {
        let packets = {
            let mut file =
                std::fs::File::open("../bins/serialized_shreds_data_complete_test.bin").unwrap();
            let mut buffer = Vec::new();
            file.read_to_end(&mut buffer).unwrap();
            Packets::try_from_slice(&buffer).unwrap()
        };
        assert_eq!(packets.packets.len(), 150_000);

        let shreds = packets
            .packets
            .iter()
            .filter_map(|p| Shred::from_payload(p.clone()).ok())
            .collect::<Vec<_>>();
        assert_eq!(shreds.len(), 149977);

        let unique_shreds = packets
            .packets
            .iter()
            .filter_map(|p| Shred::from_payload(p.clone()).ok().map(ComparableShred))
            .collect::<HashSet<ComparableShred>>();
        assert_eq!(unique_shreds.len(), 109221);

        let unique_slot_fec_shreds = packets
            .packets
            .iter()
            .filter_map(|p| {
                Shred::from_payload(p.clone())
                    .ok()
                    .map(|s| *s.common_header())
            })
            .collect::<HashSet<ShredCommonHeader>>();
        assert_eq!(unique_slot_fec_shreds.len(), 109221);

        let rs_cache = ReedSolomonCache::default();
        let metrics = Arc::new(ShredMetrics::default());

        // Test 1: all shreds provided
        let mut all_shreds = ahash::HashMap::default();
        let mut slot_fec_indexes_to_iterate: Vec<(Slot, u32)> = Vec::new();
        let mut deshredded_entries = Vec::new();
        let mut highest_slot_seen = 0;
        let recovered_count = reconstruct_shreds(
            PacketBatch::new(
                packets
                    .packets
                    .iter()
                    .map(|x| {
                        let mut packet = Packet::default();
                        packet.buffer_mut()[..x.len()].copy_from_slice(x);
                        packet.meta_mut().size = x.len();
                        packet
                    })
                    .collect_vec(),
            ),
            &mut all_shreds,
            &mut slot_fec_indexes_to_iterate,
            &mut deshredded_entries,
            &mut highest_slot_seen,
            &rs_cache,
            &metrics,
        );

        // debug_to_disk(&mut deshredded_entries);
        assert!(recovered_count < deshredded_entries.len());
        assert_decoded_entries_sane(&deshredded_entries);
        let total_entries = deshredded_entries
            .iter()
            .map(|(_slot, entries, _entries_bytes)| entries.len())
            .sum::<usize>();
        assert!(total_entries >= 43170, "total_entries: {total_entries}");
        assert_eq!(all_shreds.len(), 61);

        let slot_to_entry = deshredded_entries
            .iter()
            .into_group_map_by(|(slot, _entries, _entries_bytes)| *slot);
        // slot_to_entry
        //     .iter()
        //     .sorted_by_key(|(slot, _)| *slot)
        //     .for_each(|(slot, entry)| {
        //         println!(
        //             "slot {slot} entry count: {:?}, txn count: {}",
        //             entry.len(),
        //             entry
        //                 .iter()
        //                 .map(|(_slot, entry)| entry.transactions.len())
        //                 .sum::<usize>()
        //         );
        //     });
        assert!(
            slot_to_entry.len() >= 61,
            "slot_to_entry.len(): {}",
            slot_to_entry.len()
        );

        // Test 2: 33% of shreds missing
        let mut all_shreds = ahash::HashMap::default();
        let mut slot_fec_indexes_to_iterate: Vec<(Slot, u32)> = Vec::new();
        let mut deshredded_entries = Vec::new();
        let mut highest_slot_seen = 0;
        let recovered_count = reconstruct_shreds(
            PacketBatch::new(
                packets
                    .packets
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| (index + 1) % 3 != 0)
                    .map(|(_i, x)| {
                        let mut packet = Packet::default();
                        packet.buffer_mut()[..x.len()].copy_from_slice(x);
                        packet.meta_mut().size = x.len();
                        packet
                    })
                    .collect_vec(),
            ),
            &mut all_shreds,
            &mut slot_fec_indexes_to_iterate,
            &mut deshredded_entries,
            &mut highest_slot_seen,
            &rs_cache,
            &metrics,
        );

        // debug_to_disk(&deshredded_entries, "new.txt");
        assert!(recovered_count > 0);
        assert_decoded_entries_sane(&deshredded_entries);
        let total_entries = deshredded_entries
            .iter()
            .map(|(_slot, entries, _entries_bytes)| entries.len())
            .sum::<usize>();
        assert!(total_entries >= 43170, "total_entries: {total_entries}");
        assert!(all_shreds.len() > 15);

        let slot_to_entry = deshredded_entries
            .iter()
            .into_group_map_by(|(slot, _entries, _entries_bytes)| *slot);
        assert!(
            slot_to_entry.len() >= 61,
            "slot_to_entry.len(): {}",
            slot_to_entry.len()
        );
    }

    #[test]
    fn test_recover_shreds() {
        let mut rng = rand::thread_rng();
        let slot = 11_111;
        let leader_keypair = Arc::new(Keypair::new());
        let reed_solomon_cache = ReedSolomonCache::default();
        let shredder = Shredder::new(slot, slot - 1, 0, 0).unwrap();
        let chained_merkle_root = Some(Hash::new_from_array(rng.gen()));
        let num_entry_groups = 10;
        let num_entries = 10;
        let mut entries = Vec::new();
        let mut data_shreds = Vec::new();
        let mut coding_shreds = Vec::new();

        let mut index = 0;
        (0..num_entry_groups).for_each(|_i| {
            let _entries = make_slot_entries_with_transactions(num_entries);
            let (_data_shreds, _coding_shreds) = shredder.entries_to_shreds(
                &leader_keypair,
                _entries.as_slice(),
                true,
                chained_merkle_root,
                index as u32, // next_shred_index
                index as u32, // next_code_index,
                true,         // merkle_variant
                &reed_solomon_cache,
                &mut ProcessShredsStats::default(),
            );
            index += _data_shreds.len();
            entries.extend(_entries);
            data_shreds.extend(_data_shreds);
            coding_shreds.extend(_coding_shreds);
        });

        let packets = data_shreds
            .iter()
            .chain(coding_shreds.iter())
            .map(|s| {
                let mut p = Packet::default();
                s.copy_to_packet(&mut p);
                p
            })
            .collect_vec();
        assert_eq!(data_shreds.len(), 320);
        assert_eq!(
            data_shreds
                .iter()
                .map(|s| s.fec_set_index())
                .dedup()
                .count(),
            num_entry_groups
        );

        let metrics = Arc::new(ShredMetrics::default());
        let rs_cache = ReedSolomonCache::default();

        // Test 1: all shreds provided
        let mut all_shreds = ahash::HashMap::default();
        let mut slot_fec_indexes_to_iterate: Vec<(Slot, u32)> = Vec::new();
        let mut deshredded_entries = Vec::new();
        let mut highest_slot_seen = 0;
        let recovered_count = reconstruct_shreds(
            PacketBatch::new(packets.clone()),
            &mut all_shreds,
            &mut slot_fec_indexes_to_iterate,
            &mut deshredded_entries,
            &mut highest_slot_seen,
            &rs_cache,
            &metrics,
        );
        assert_eq!(recovered_count, 0);
        assert_eq!(
            deshredded_entries
                .iter()
                .map(|(_slot, entries, _entries_bytes)| entries.len())
                .sum::<usize>(),
            entries.len()
        );
        assert_eq!(
            all_shreds.len(),
            1, // slot 11111
        );

        // Test 2: 33% of shreds missing
        let mut all_shreds = ahash::HashMap::default();
        let mut slot_fec_indexes_to_iterate: Vec<(Slot, u32)> = Vec::new();
        let mut deshredded_entries = Vec::new();
        let mut highest_slot_seen = 0;
        let recovered_count = reconstruct_shreds(
            PacketBatch::new(
                packets
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| (index + 1) % 3 != 0)
                    .map(|(_i, p)| p.clone())
                    .collect(),
            ),
            &mut all_shreds,
            &mut slot_fec_indexes_to_iterate,
            &mut deshredded_entries,
            &mut highest_slot_seen,
            &rs_cache,
            &metrics,
        );
        assert!(recovered_count > 0);
        assert_eq!(
            deshredded_entries
                .iter()
                .map(|(_slot, entries, _entries_bytes)| entries.len())
                .sum::<usize>(),
            entries.len()
        );
        assert_eq!(
            all_shreds.len(),
            1, // slot 11111
        );
    }
}
#[cfg(test)]
mod get_indexes_tests {
    use super::{get_indexes, ShredStatus, ShredsStateTracker};

    fn make_test_statustracker(statuses: &[ShredStatus]) -> ShredsStateTracker {
        let mut tracker = ShredsStateTracker::default();
        tracker.data_status[..statuses.len()].copy_from_slice(statuses);
        tracker
    }

    #[test]
    fn start_at_index_zero() {
        let s = [
            ShredStatus::NotDataComplete,
            ShredStatus::NotDataComplete,
            ShredStatus::DataComplete,
        ];
        let tracker = make_test_statustracker(&s);
        assert_eq!(get_indexes(&tracker, 0), Some((0, 2, false)));

        let s = [
            ShredStatus::DataComplete,
            ShredStatus::NotDataComplete,
            ShredStatus::DataComplete,
        ];
        let tracker = make_test_statustracker(&s);
        assert_eq!(get_indexes(&tracker, 0), Some((0, 0, false)));

        let s = [
            ShredStatus::Unknown,
            ShredStatus::NotDataComplete,
            ShredStatus::DataComplete,
        ];
        let tracker = make_test_statustracker(&s);
        assert_eq!(get_indexes(&tracker, 0), None);
    }

    #[test]
    fn start_just_after_data_complete() {
        let s = [
            ShredStatus::DataComplete,
            ShredStatus::NotDataComplete,
            ShredStatus::NotDataComplete,
            ShredStatus::DataComplete,
        ];
        let tracker = make_test_statustracker(&s);
        assert_eq!(get_indexes(&tracker, 1), Some((1, 3, false)));
    }

    #[test]
    fn start_just_before_data_complete() {
        let s = [
            ShredStatus::DataComplete,
            ShredStatus::NotDataComplete,
            ShredStatus::DataComplete,
        ];
        let tracker = make_test_statustracker(&s);
        assert_eq!(get_indexes(&tracker, 1), Some((1, 2, false)));
    }

    #[test]
    fn two_consecutive_data_complete() {
        let s = [
            ShredStatus::NotDataComplete,
            ShredStatus::DataComplete,
            ShredStatus::DataComplete,
        ];
        let tracker = make_test_statustracker(&s);
        assert_eq!(get_indexes(&tracker, 1), Some((0, 1, false)));
        assert_eq!(get_indexes(&tracker, 2), Some((2, 2, false)));
    }

    #[test]
    fn three_consecutive_data_complete() {
        let s = [
            ShredStatus::NotDataComplete,
            ShredStatus::DataComplete,
            ShredStatus::DataComplete,
            ShredStatus::DataComplete,
            ShredStatus::NotDataComplete,
        ];
        let tracker = make_test_statustracker(&s);
        assert_eq!(get_indexes(&tracker, 1), Some((0, 1, false)));
        assert_eq!(get_indexes(&tracker, 2), Some((2, 2, false)));
        assert_eq!(get_indexes(&tracker, 3), Some((3, 3, false)));
    }

    #[test]
    fn unknown_discards_segment() {
        let s = [
            ShredStatus::NotDataComplete,
            ShredStatus::Unknown,
            ShredStatus::DataComplete,
        ];
        let tracker = make_test_statustracker(&s);
        assert_eq!(get_indexes(&tracker, 0), None);

        let s = [
            ShredStatus::Unknown,
            ShredStatus::NotDataComplete,
            ShredStatus::DataComplete,
        ];
        let tracker = make_test_statustracker(&s);
        assert_eq!(get_indexes(&tracker, 1), Some((1, 2, true)));
    }

    #[test]
    fn test_unknown() {
        let s = [
            ShredStatus::Unknown,
            ShredStatus::DataComplete,
            ShredStatus::DataComplete,
            ShredStatus::NotDataComplete,
            ShredStatus::DataComplete,
        ];
        let tracker = make_test_statustracker(&s);
        assert_eq!(get_indexes(&tracker, 0), None);
        assert_eq!(get_indexes(&tracker, 1), Some((1, 1, true)));
        assert_eq!(get_indexes(&tracker, 2), Some((2, 2, false)));
        assert_eq!(get_indexes(&tracker, 3), Some((3, 4, false)));
    }
}
