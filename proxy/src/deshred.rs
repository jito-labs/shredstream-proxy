//! High-throughput shred reconstruction pipeline:
//! - ingest/validate Merkle shreds,
//! - perform per-FEC recovery when profitable,
//! - deshred completed data ranges into serialized `Vec<Entry>` bytes.

use std::{
    io::Cursor,
    sync::{atomic::Ordering, Arc},
    time::Instant,
};

use bincode::Options;
use bitvec::prelude::{BitVec, Lsb0};
use itertools::Itertools;
use jito_protos::shredstream::TraceShred;
use log::{debug, warn};
use prost::Message;
use solana_ledger::{
    blockstore::MAX_DATA_SHREDS_PER_SLOT,
    shred::{
        merkle::{Shred, ShredCode as MerkleCodeShred, ShredData as MerkleDataShred},
        traits::Shred as ShredTrait,
        ReedSolomonCache, ShredType, Shredder,
    },
};
use solana_metrics::{datapoint_info, datapoint_warn};
use solana_perf::packet::PacketBatch;
use solana_sdk::{
    clock::{Slot, MAX_PROCESSING_AGE},
    signature::Signature,
};

use crate::forwarder::ShredMetrics;

/// Identity for one erasure batch within a slot.
///
/// We key by `(fec_set_index, version, leader_signature)` so conflicting duplicates
/// from different shred versions/signatures do not share the same recovery state.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub(crate) struct FecSetKey {
    /// Start index of this FEC set within the slot.
    pub fec_set_index: u32,
    /// Cluster shred version from common header.
    pub version: u16,
    /// Leader signature bytes over the FEC set's Merkle root.
    pub signature: Signature,
}

impl FecSetKey {
    #[inline]
    /// Build a recovery key from shred common-header fields.
    fn from_shred(shred: &Shred) -> Self {
        let h = shred.common_header();
        Self {
            fec_set_index: h.fec_set_index,
            version: h.version,
            signature: h.signature,
        }
    }
}

/// Bounds for per-call slot acceptance around the median slot anchor.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ReconstructShredsConfig {
    /// Accept shreds this many slots behind the current slot anchor.
    pub slot_lookback: Slot,
    /// Accept shreds this many slots ahead of the current slot anchor.
    pub slot_future: Slot,
    /// Maximum number of unknown-start candidate positions to try per end boundary.
    pub unknown_start_max_positions: u16,
    /// Enable known-start parity scrub after a known-boundary decode failure.
    pub known_start_parity_scrub_enabled: bool,
    /// Max overlapping FEC sets to scrub per known-start decode failure.
    pub known_start_parity_scrub_max_fec_sets_per_failure: u16,
    /// Max data indexes to force-missing per scrubbed FEC set (start/mid/end ordering).
    pub known_start_parity_scrub_max_indices_per_fec: u16,
    /// Max scrub attempts allowed for one FEC identity generation.
    pub known_start_parity_scrub_max_attempts_per_fec_generation: u8,
}

/// Reusable allocation scratch for `reconstruct_shreds`.
#[derive(Default)]
pub(crate) struct ReconstructScratch {
    /// Version histogram used to select dominant ingress version per call.
    version_counts: ahash::HashMap<u16, u32>,
    /// Reused Merkle variant-profile histogram for per-FEC recovery filtering.
    variant_profile_counts: ahash::HashMap<(u8, bool, bool), usize>,
    /// Parsed shreds awaiting slot-window filtering/ingest.
    parsed_shreds: Vec<(Shred, Slot, ShredIndex, FecSetKey)>,
    /// Slot samples used to compute the median slot anchor.
    slot_samples: Vec<Slot>,
    /// Snapshot of DATA_COMPLETE indices for phase-3 iteration.
    data_complete_idxs: Vec<ShredIndex>,
    /// Candidate unknown-start positions for one DATA_COMPLETE end boundary.
    unknown_start_candidates: Vec<ShredIndex>,
    /// Reused suffix-missing counters for unknown-start candidate ranking.
    ///
    /// `unknown_start_missing_suffix[i]` stores the number of missing indices in the suffix
    /// starting at `window_start + i` and ending at the current end boundary.
    unknown_start_missing_suffix: Vec<u16>,
    /// Reused unique overlapping FEC identities for known-start scrub.
    known_start_scrub_fec_keys: Vec<FecSetKey>,
    /// Reused forced-missing candidate indices for one scrubbed FEC identity.
    known_start_scrub_candidate_indices: Vec<usize>,
}

/// Narrow index type for shred positions bounded by `MAX_DATA_SHREDS_PER_SLOT`.
type ShredIndex = u16;

const _: () = assert!(MAX_DATA_SHREDS_PER_SLOT <= u16::MAX as usize);

/// Source label used only for conflict metrics when a data shred is replaced.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DataShredSource {
    /// Shred came directly from ingress packets.
    Incoming,
    /// Shred was produced by Reed-Solomon recovery.
    Recovered,
}

#[derive(Clone, Debug, Default)]
struct RecoverFecOutcome {
    recovered_data_shreds: usize,
    recover_failed: bool,
}

#[derive(Debug)]
enum DecodeEntriesError {
    Deshred(solana_ledger::shred::Error),
    MissingShred {
        relative_index: usize,
    },
    Deserialize {
        payload_len: usize,
        error: Box<bincode::ErrorKind>,
    },
    EntrySanity {
        entries_len: usize,
    },
}

/// Metadata for one provisional unknown-start emission tied to an end boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct UnknownStartEmission {
    /// Candidate start boundary used for the provisional decode.
    start_data_complete_idx: ShredIndex,
    /// Max per-index generation within `[start_data_complete_idx, end_data_complete_idx]` at
    /// emission time.
    ///
    /// If this changes before canonical completion, we must re-decode on commit.
    range_generation: u16,
}

/// Count byte-level differences between two buffers (including length delta).
#[inline]
fn count_byte_diffs(a: &[u8], b: &[u8]) -> u64 {
    let len = std::cmp::min(a.len(), b.len());
    let mut diffs = 0u64;
    for i in 0..len {
        diffs += (a[i] != b[i]) as u64;
    }
    diffs + (a.len().max(b.len()) - len) as u64
}

/// Normalize payload bytes for duplicate/conflict comparisons.
///
/// Resigned Merkle shreds include a retransmitter signature suffix that can differ across
/// forwarders while the leader-signed shred content is otherwise identical. Ignore that suffix
/// when deciding whether two shreds are conflicting.
#[inline]
fn payload_compare_prefix(payload: &[u8], variant: solana_ledger::shred::ShredVariant) -> &[u8] {
    let (tag, _proof_size, _chained, resigned) = merkle_variant_fields(variant);
    if resigned && matches!(tag, 0x70 | 0xB0) {
        let len = payload
            .len()
            .saturating_sub(solana_ledger::shred::SIZE_OF_SIGNATURE);
        &payload[..len]
    } else {
        payload
    }
}

/// Normalize `ShredVariant` into `(tag, proof_size, chained, resigned)` for logs/comparisons.
#[inline]
fn merkle_variant_fields(variant: solana_ledger::shred::ShredVariant) -> (u8, u8, bool, bool) {
    use solana_ledger::shred::ShredVariant;

    match variant {
        ShredVariant::MerkleCode {
            proof_size,
            chained,
            resigned,
        } => {
            let tag = if !chained {
                0x40
            } else if resigned {
                0x70
            } else {
                0x60
            };
            (tag, proof_size, chained, resigned)
        }
        ShredVariant::MerkleData {
            proof_size,
            chained,
            resigned,
        } => {
            let tag = if !chained {
                0x80
            } else if resigned {
                0xB0
            } else {
                0x90
            };
            (tag, proof_size, chained, resigned)
        }
        ShredVariant::LegacyCode => (u8::from(ShredType::Code), u8::MAX, false, false),
        ShredVariant::LegacyData => (u8::from(ShredType::Data), u8::MAX, false, false),
    }
}

/// Tracks per-slot shred information for data shreds
/// Guaranteed to have MAX_DATA_SHREDS_PER_SLOT entries in each per-index tracker
#[derive(Debug)]
pub struct ShredsStateTracker {
    /// Marks indices whose shred has DATA_COMPLETE_SHRED (or LAST_SHRED_IN_SLOT) set.
    data_complete: BitVec<u64, Lsb0>,
    /// Data shreds received for the slot (not coding!)
    data_shreds: Vec<Option<Shred>>,
    /// Tracks which FEC set indexes have already been fully recovered.
    already_recovered_fec_sets: BitVec<u64, Lsb0>,
    /// Tracks which data shred indexes have already been deshredded/consumed.
    already_deshredded: BitVec<u64, Lsb0>,
    /// Tracks which `DATA_COMPLETE_SHRED` end indices we've already emitted an "unknown start"
    /// decode for (and what start index we used).
    /// Keyed by `end_data_complete_idx`.
    /// This prevents repeatedly emitting the same best-effort decode while still allowing a later
    /// correct decode (with a different start boundary) if/when the missing boundary shred arrives.
    unknown_start_emitted: ahash::HashMap<ShredIndex, UnknownStartEmission>,
    /// Last data-range generation we attempted for each unknown-start end boundary.
    ///
    /// Used to skip repeating speculative decode attempts when no data payload in the candidate
    /// range changed since the last attempt.
    unknown_start_last_attempt_generation: ahash::HashMap<ShredIndex, u16>,
    /// Monotonic counter bumped whenever any data shred payload for this slot is inserted/replaced.
    data_generation: u16,
    /// Per-index generation stamp for the last accepted payload at that data index.
    data_index_generation: Vec<u16>,
}
impl Default for ShredsStateTracker {
    fn default() -> Self {
        Self {
            data_complete: BitVec::repeat(false, MAX_DATA_SHREDS_PER_SLOT),
            data_shreds: vec![None; MAX_DATA_SHREDS_PER_SLOT],
            already_recovered_fec_sets: BitVec::repeat(false, MAX_DATA_SHREDS_PER_SLOT),
            already_deshredded: BitVec::repeat(false, MAX_DATA_SHREDS_PER_SLOT),
            unknown_start_emitted: ahash::HashMap::default(),
            unknown_start_last_attempt_generation: ahash::HashMap::default(),
            data_generation: 0,
            data_index_generation: vec![0u16; MAX_DATA_SHREDS_PER_SLOT],
        }
    }
}

impl ShredsStateTracker {
    #[inline]
    fn range_generation_max(
        &self,
        start_data_complete_idx: ShredIndex,
        end_data_complete_idx: ShredIndex,
    ) -> u16 {
        debug_assert!(start_data_complete_idx <= end_data_complete_idx);
        debug_assert!((end_data_complete_idx as usize) < self.data_index_generation.len());
        self.data_index_generation
            [usize::from(start_data_complete_idx)..=usize::from(end_data_complete_idx)]
            .iter()
            .max()
            .copied()
            .unwrap_or_default()
    }
}

const BINCODE_DESERIALIZE_LIMIT_BYTES: usize =
    MAX_DATA_SHREDS_PER_SLOT * MerkleDataShred::SIZE_OF_PAYLOAD;

/// Cheap hash-chain sanity for unknown-start decodes to reject obvious garbage.
fn entries_pass_basic_sanity(entries: &[solana_entry::entry::Entry]) -> bool {
    // Only do cheap checks: this is called on best-effort (unknown-start) decodes.
    if entries.len() < 2 {
        return true;
    }

    // Prefix transition.
    if !entries[1].verify(&entries[0].hash) {
        return false;
    }

    // Middle transition.
    let mid = entries.len() / 2;
    if mid > 0 && !entries[mid].verify(&entries[mid - 1].hash) {
        return false;
    }

    // Suffix transition.
    let last = entries.len() - 1;
    if !entries[last].verify(&entries[last - 1].hash) {
        return false;
    }

    true
}

/// Recovery bookkeeping for one exact FEC identity.
#[derive(Debug, Default)]
pub(crate) struct FecSetState {
    // Coding shreds keyed by `position` within the coding set.
    coding_by_pos: Vec<Option<Shred>>,
    // Erasure-set width learned from coding headers.
    num_data_shreds: u16,
    // Parity-set width learned from coding headers.
    num_coding_shreds: u16,
    // Number of coding positions currently populated.
    coding_count: u16,
    // Number of matching data shreds currently present for this exact FEC identity
    // (slot + fec_set_index + version + signature). Populated once when coding arrives,
    // then maintained incrementally as new data shreds are inserted/overwritten.
    data_count: u16,
    data_count_initialized: bool,
    // Skip repeated recover attempts when no new shards have arrived.
    last_recover_shard_count: u16,
    // Monotonic change counter for this FEC set identity. Incremented whenever data/coding
    // membership or payload changes for this key.
    generation: u64,
    // Generation observed at the last recovery attempt.
    last_recover_generation: u64,
    // Generation tracked for known-start parity scrub budgeting.
    known_start_scrub_generation: u64,
    // Number of known-start parity scrub attempts used in `known_start_scrub_generation`.
    known_start_scrub_attempts_in_generation: u8,
    // First time we observed any shred (data or coding) for this exact FEC identity.
    first_shred_seen_at: Option<Instant>,
    // Whether we've already recorded unknown-start completion latency for this FEC identity.
    unknown_start_decode_completed: bool,
    // Whether we've already recorded known-start completion latency for this FEC identity.
    known_start_decode_completed: bool,
}

impl FecSetState {
    /// Insert/replace one coding shred by coding `position`.
    /// Returns true if this call mutated set membership or payload bytes.
    fn insert_coding_shred(&mut self, shred: Shred) -> bool {
        let Shred::ShredCode(s) = &shred else {
            return false;
        };
        let position = s.coding_header.position as usize;
        let num_coding_shreds = s.coding_header.num_coding_shreds as usize;

        // First coding shred defines the erasure params for this exact FEC identity.
        if self.num_data_shreds == 0 {
            self.num_data_shreds = s.coding_header.num_data_shreds;
            self.num_coding_shreds = s.coding_header.num_coding_shreds;
            self.coding_by_pos.resize(num_coding_shreds, None);
        } else {
            if self.num_data_shreds != s.coding_header.num_data_shreds
                || self.num_coding_shreds != s.coding_header.num_coding_shreds
            {
                // Inputs are assumed filtered; if this happens anyway, keep the first view of the set.
                let (new_tag, new_proof, new_chained, new_resigned) =
                    merkle_variant_fields(s.common_header.shred_variant);
                let (
                    existing_sig_diff,
                    existing_version,
                    existing_tag,
                    existing_proof,
                    existing_chained,
                    existing_resigned,
                ) = self
                    .coding_by_pos
                    .iter()
                    .filter_map(|x| x.as_ref())
                    .find_map(|existing| match existing {
                        Shred::ShredCode(old) => {
                            let sig_diff = count_byte_diffs(
                                old.common_header.signature.as_ref(),
                                s.common_header.signature.as_ref(),
                            );
                            let (old_tag, old_proof, old_chained, old_resigned) =
                                merkle_variant_fields(old.common_header.shred_variant);
                            Some((
                                sig_diff,
                                old.common_header.version as i64,
                                old_tag,
                                old_proof,
                                old_chained,
                                old_resigned,
                            ))
                        }
                        Shred::ShredData(_) => None,
                    })
                    .unwrap_or((0, -1, 0, u8::MAX, false, false));

                datapoint_info!(
                    "shredstream_proxy-deshred_shred_conflict",
                    "kind" => "code",
                    "reason" => "coding_header_mismatch",
                    ("slot", s.common_header.slot, i64),
                    ("fec_set_index", s.common_header.fec_set_index, i64),
                    ("position", s.coding_header.position, i64),
                    ("payload_len_new", s.payload.len(), i64),
                    ("num_data_expected", self.num_data_shreds, i64),
                    ("num_data_new", s.coding_header.num_data_shreds, i64),
                    ("num_coding_expected", self.num_coding_shreds, i64),
                    ("num_coding_new", s.coding_header.num_coding_shreds, i64),
                    ("existing_version", existing_version, i64),
                    ("new_version", s.common_header.version, i64),
                    ("sig_diff_bytes_vs_existing", existing_sig_diff, i64),
                    ("variant_tag_existing", existing_tag, i64),
                    ("variant_tag_new", new_tag, i64),
                    ("proof_size_existing", existing_proof, i64),
                    ("proof_size_new", new_proof, i64),
                    ("chained_existing", existing_chained, bool),
                    ("chained_new", new_chained, bool),
                    ("resigned_existing", existing_resigned, bool),
                    ("resigned_new", new_resigned, bool),
                );
                return false;
            }
            if self.coding_by_pos.len() < num_coding_shreds {
                self.coding_by_pos.resize(num_coding_shreds, None);
            }
        }

        if position >= self.coding_by_pos.len() {
            // Malformed coding shred (position out of bounds).
            datapoint_info!(
                "shredstream_proxy-deshred_shred_conflict",
                "kind" => "code",
                "reason" => "position_oob",
                ("slot", s.common_header.slot, i64),
                ("fec_set_index", s.common_header.fec_set_index, i64),
                ("position", s.coding_header.position, i64),
                ("coding_by_pos_len", self.coding_by_pos.len(), i64),
                ("num_data_shreds", s.coding_header.num_data_shreds, i64),
                ("num_coding_shreds", s.coding_header.num_coding_shreds, i64),
                ("payload_len", s.payload.len(), i64),
            );
            return false;
        }
        let Some(existing) = self.coding_by_pos[position].as_ref() else {
            self.coding_by_pos[position] = Some(shred);
            self.coding_count = self.coding_count.saturating_add(1);
            self.generation = self.generation.wrapping_add(1);
            return true;
        };
        let Shred::ShredCode(old) = existing else {
            return false;
        };

        let sig_diff = count_byte_diffs(
            old.common_header.signature.as_ref(),
            s.common_header.signature.as_ref(),
        );
        let (old_tag, old_proof, old_chained, old_resigned) =
            merkle_variant_fields(old.common_header.shred_variant);
        let (new_tag, new_proof, new_chained, new_resigned) =
            merkle_variant_fields(s.common_header.shred_variant);
        let old_payload =
            payload_compare_prefix(old.payload.as_ref(), old.common_header.shred_variant);
        let new_payload = payload_compare_prefix(s.payload.as_ref(), s.common_header.shred_variant);
        let payload_byte_diff = count_byte_diffs(old_payload, new_payload);

        let is_conflict = sig_diff != 0
            || old.common_header.version != s.common_header.version
            || old.common_header.index != s.common_header.index
            || old.common_header.fec_set_index != s.common_header.fec_set_index
            || old.coding_header.num_data_shreds != s.coding_header.num_data_shreds
            || old.coding_header.num_coding_shreds != s.coding_header.num_coding_shreds
            || old.coding_header.position != s.coding_header.position
            || old_tag != new_tag
            || old_proof != new_proof
            || old.payload.len() != s.payload.len()
            || payload_byte_diff != 0;
        if !is_conflict {
            return false;
        }

        datapoint_info!(
            "shredstream_proxy-deshred_shred_conflict",
            "kind" => "code",
            "reason" => "duplicate_position",
            ("slot", s.common_header.slot, i64),
            ("fec_set_index_old", old.common_header.fec_set_index, i64),
            ("fec_set_index_new", s.common_header.fec_set_index, i64),
            ("position", s.coding_header.position, i64),
            ("index_old", old.common_header.index, i64),
            ("index_new", s.common_header.index, i64),
            ("payload_len_old", old.payload.len(), i64),
            ("payload_len_new", s.payload.len(), i64),
            ("payload_len_diff", s.payload.len().abs_diff(old.payload.len()), i64),
            ("payload_diff_bytes", payload_byte_diff, i64),
            ("version_old", old.common_header.version, i64),
            ("version_new", s.common_header.version, i64),
            ("sig_diff_bytes", sig_diff, i64),
            ("variant_tag_old", old_tag, i64),
            ("variant_tag_new", new_tag, i64),
            ("proof_size_old", old_proof, i64),
            ("proof_size_new", new_proof, i64),
            ("chained_old", old_chained, bool),
            ("chained_new", new_chained, bool),
            ("resigned_old", old_resigned, bool),
            ("resigned_new", new_resigned, bool),
            ("num_data_shreds_old", old.coding_header.num_data_shreds, i64),
            ("num_data_shreds_new", s.coding_header.num_data_shreds, i64),
            ("num_coding_shreds_old", old.coding_header.num_coding_shreds, i64),
            ("num_coding_shreds_new", s.coding_header.num_coding_shreds, i64),
        );

        self.coding_by_pos[position] = Some(shred);
        self.generation = self.generation.wrapping_add(1);
        true
    }
}

#[inline]
fn merkle_payload_size_from_tag(tag: u8) -> Option<usize> {
    match tag & 0xF0 {
        // MerkleCode: 0x40 (unchained), 0x60 (chained), 0x70 (chained resigned)
        0x40 | 0x60 | 0x70 => Some(<MerkleCodeShred as ShredTrait>::SIZE_OF_PAYLOAD),
        // MerkleData: 0x80 (unchained), 0x90 (chained), 0xB0 (chained resigned)
        0x80 | 0x90 | 0xB0 => Some(<MerkleDataShred as ShredTrait>::SIZE_OF_PAYLOAD),
        _ => None,
    }
}

/// Keep `last_recover_shard_count` bounded by currently tracked (data + coding) shards.
#[inline]
fn cap_last_recover_shard_count(fec_state: &mut FecSetState) {
    let total = fec_state.data_count.saturating_add(fec_state.coding_count);
    fec_state.last_recover_shard_count = fec_state.last_recover_shard_count.min(total);
}

/// Return true when this shred belongs to the exact FEC identity key.
#[inline]
fn shred_matches_fec_key(shred: &Shred, key: &FecSetKey) -> bool {
    let h = shred.common_header();
    h.fec_set_index == key.fec_set_index && h.version == key.version && h.signature == key.signature
}

/// Count present data shreds in this FEC set that match the exact FEC identity.
#[inline]
fn matching_data_count_for_fec_key(
    state_tracker: &ShredsStateTracker,
    key: &FecSetKey,
    num_data_shreds: u16,
) -> u16 {
    let start = key.fec_set_index as usize;
    let Some(end_excl) = start.checked_add(num_data_shreds as usize) else {
        return 0;
    };
    if end_excl > state_tracker.data_shreds.len() {
        return 0;
    }

    let mut matches = 0u16;
    for idx in start..end_excl {
        if state_tracker.data_shreds[idx]
            .as_ref()
            .is_some_and(|s| shred_matches_fec_key(s, key))
        {
            matches = matches.saturating_add(1);
        }
    }
    matches
}

/// Lazily initialize `data_count` once coding metadata defines the expected data-shred span.
#[inline]
fn maybe_init_fec_data_count(
    fec_state: &mut FecSetState,
    key: &FecSetKey,
    state_tracker: &ShredsStateTracker,
) {
    if fec_state.data_count_initialized || fec_state.num_data_shreds == 0 {
        return;
    }
    fec_state.data_count =
        matching_data_count_for_fec_key(state_tracker, key, fec_state.num_data_shreds);
    fec_state.data_count_initialized = true;
    cap_last_recover_shard_count(fec_state);
}

/// Apply data-count deltas when a data shred at one index changes FEC identity.
#[inline]
fn update_fec_state_after_data_shred_change(
    fec_sets: &mut ahash::HashMap<FecSetKey, FecSetState>,
    old_key: Option<FecSetKey>,
    new_key: Option<FecSetKey>,
) {
    if old_key == new_key {
        if let Some(key) = old_key {
            bump_fec_state_generation(fec_sets, key);
        }
        return;
    }

    let mut adjust_and_bump = |key: FecSetKey, is_increment: bool| {
        if let Some(state) = fec_sets
            .get_mut(&key)
            .filter(|state| state.data_count_initialized)
        {
            if is_increment {
                state.data_count = state.data_count.saturating_add(1);
            } else {
                state.data_count = state.data_count.saturating_sub(1);
                cap_last_recover_shard_count(state);
            }
        }
        bump_fec_state_generation(fec_sets, key);
    };

    if let Some(old_key) = old_key {
        adjust_and_bump(old_key, false);
    }
    if let Some(new_key) = new_key {
        adjust_and_bump(new_key, true);
    }
}

/// Mark the first-seen timestamp for one FEC identity.
#[inline]
fn mark_fec_set_first_shred_seen(
    fec_sets: &mut ahash::HashMap<FecSetKey, FecSetState>,
    key: FecSetKey,
    first_shred_observed_at: Instant,
) {
    let fec_state = fec_sets.entry(key).or_default();
    fec_state
        .first_shred_seen_at
        .get_or_insert(first_shred_observed_at);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DecodeCompletionKind {
    UnknownStart,
    KnownStart,
}

/// Record first-shred-to-completion latency for each FEC identity in a decoded data range.
///
/// We record at most one unknown-start and one known-start completion per FEC identity.
fn observe_fec_set_decode_completion_latency(
    fec_sets: &mut ahash::HashMap<FecSetKey, FecSetState>,
    metrics: &ShredMetrics,
    to_deshred: &[Option<Shred>],
    completion_kind: DecodeCompletionKind,
) {
    let completion_observed_at = Instant::now();
    let mut prev_fec_key: Option<FecSetKey> = None;
    for shred in to_deshred.iter().filter_map(|s| s.as_ref()) {
        let fec_key = FecSetKey::from_shred(shred);
        if Some(fec_key) == prev_fec_key {
            continue;
        }
        prev_fec_key = Some(fec_key);
        let Some(fec_state) = fec_sets.get_mut(&fec_key) else {
            continue;
        };
        let already_recorded = match completion_kind {
            DecodeCompletionKind::UnknownStart => fec_state.unknown_start_decode_completed,
            DecodeCompletionKind::KnownStart => fec_state.known_start_decode_completed,
        };
        if already_recorded {
            continue;
        }
        let Some(first_seen_at) = fec_state.first_shred_seen_at else {
            continue;
        };
        let elapsed_us = completion_observed_at
            .saturating_duration_since(first_seen_at)
            .as_micros()
            .min(u64::MAX as u128) as u64;
        match completion_kind {
            DecodeCompletionKind::UnknownStart => {
                metrics
                    .fec_set_decode_unknown_start_latency_us_sum
                    .fetch_add(elapsed_us, Ordering::Relaxed);
                metrics
                    .fec_set_decode_unknown_start_latency_count
                    .fetch_add(1, Ordering::Relaxed);
                fec_state.unknown_start_decode_completed = true;
            }
            DecodeCompletionKind::KnownStart => {
                metrics
                    .fec_set_decode_known_start_latency_us_sum
                    .fetch_add(elapsed_us, Ordering::Relaxed);
                metrics
                    .fec_set_decode_known_start_latency_count
                    .fetch_add(1, Ordering::Relaxed);
                fec_state.known_start_decode_completed = true;
            }
        }
    }
}

/// Commit a decoded data range and opportunistically free memory for completed FEC sets.
fn commit_deshredded_range(
    fec_sets: &mut ahash::HashMap<FecSetKey, FecSetState>,
    state_tracker: &mut ShredsStateTracker,
    start_data_complete_idx: ShredIndex,
    end_data_complete_idx: ShredIndex,
) {
    let start_data_complete_idx = start_data_complete_idx as usize;
    let end_data_complete_idx = end_data_complete_idx as usize;
    for idx in start_data_complete_idx..=end_data_complete_idx {
        state_tracker.already_deshredded.set(idx, true);
    }

    // Mark completed FEC sets and free coding shreds early.
    let mut prev_fec_key: Option<FecSetKey> = None;
    for idx in start_data_complete_idx..=end_data_complete_idx {
        let Some(fec_key) = state_tracker.data_shreds[idx]
            .as_ref()
            .map(FecSetKey::from_shred)
        else {
            continue;
        };
        if Some(fec_key) == prev_fec_key {
            continue;
        }
        prev_fec_key = Some(fec_key);
        let fec_idx = fec_key.fec_set_index as usize;
        if state_tracker.already_recovered_fec_sets[fec_idx] {
            continue;
        }
        let Some(num_data) = fec_sets
            .get(&fec_key)
            .filter(|fs| fs.num_data_shreds > 0)
            .map(|fs| fs.num_data_shreds)
        else {
            continue;
        };
        if has_all_data_shreds_for_fec_set(state_tracker, &fec_key, num_data) {
            state_tracker.already_recovered_fec_sets.set(fec_idx, true);
            let start = fec_key.fec_set_index as usize;
            let end_excl = start + (num_data as usize);
            for idx in start..end_excl {
                if state_tracker.already_deshredded[idx] {
                    state_tracker.data_shreds[idx] = None;
                }
            }
            if let Some(fec_state) = fec_sets.get_mut(&fec_key) {
                fec_state.coding_by_pos.clear();
                fec_state.coding_count = 0;
                cap_last_recover_shard_count(fec_state);
            }
        }
    }

    // Drop data payloads for indices whose FEC set is now recovered.
    for idx in start_data_complete_idx..=end_data_complete_idx {
        let should_drop = state_tracker.data_shreds[idx].as_ref().is_some_and(|s| {
            state_tracker.already_recovered_fec_sets[s.fec_set_index() as usize]
        });
        if should_drop {
            state_tracker.data_shreds[idx] = None;
        }
    }
}

fn decode_entries_from_shred_range(
    to_deshred: &[Option<Shred>],
    enforce_entry_sanity: bool,
) -> Result<(Vec<solana_entry::entry::Entry>, Vec<u8>), DecodeEntriesError> {
    if let Some(relative_index) = to_deshred.iter().position(Option::is_none) {
        return Err(DecodeEntriesError::MissingShred { relative_index });
    }
    // Safe after the explicit precheck above and avoids building a temporary Vec of payload refs.
    let deshredded_payload = Shredder::deshred(
        to_deshred
            .iter()
            .map(|s| s.as_ref().expect("prechecked Some").payload()),
    )
    .map_err(DecodeEntriesError::Deshred)?;

    let mut cursor = Cursor::new(&deshredded_payload);
    let entries = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(BINCODE_DESERIALIZE_LIMIT_BYTES as u64)
        .allow_trailing_bytes()
        .deserialize_from::<_, Vec<solana_entry::entry::Entry>>(&mut cursor)
        .map_err(|e| DecodeEntriesError::Deserialize {
            payload_len: deshredded_payload.len(),
            error: e,
        })?;

    if enforce_entry_sanity && !entries_pass_basic_sanity(&entries) {
        return Err(DecodeEntriesError::EntrySanity {
            entries_len: entries.len(),
        });
    }

    Ok((entries, deshredded_payload))
}

#[inline]
fn store_data_shred_at_index(
    state_tracker: &mut ShredsStateTracker,
    index: usize,
    is_data_complete: bool,
    shred: Shred,
) {
    state_tracker.data_complete.set(index, is_data_complete);
    state_tracker.data_shreds[index] = Some(shred);
    state_tracker.data_generation = state_tracker.data_generation.wrapping_add(1);
    state_tracker.data_index_generation[index] = state_tracker.data_generation;
}

fn record_decode_entries_error(
    metrics: &ShredMetrics,
    slot: Slot,
    start_data_complete_idx: ShredIndex,
    end_data_complete_idx: ShredIndex,
    unknown_start: bool,
    err: &DecodeEntriesError,
) {
    let decode_kind = if unknown_start {
        "unknown-start"
    } else {
        "known-start"
    };

    match err {
        DecodeEntriesError::Deshred(error) => {
            warn!(
                "slot {slot} failed to deshred {decode_kind} range, start_data_complete_idx: {start_data_complete_idx}, end_data_complete_idx: {end_data_complete_idx}. Err: {error}"
            );
            metrics.deshred_error_count.fetch_add(1, Ordering::Relaxed);
            if unknown_start {
                metrics
                    .deshred_error_unknown_start_count
                    .fetch_add(1, Ordering::Relaxed);
                metrics
                    .unknown_start_position_error_count
                    .fetch_add(1, Ordering::Relaxed);
            } else {
                metrics
                    .deshred_error_known_start_count
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
        DecodeEntriesError::MissingShred { relative_index } => {
            warn!(
                "slot {slot} missing shred during {decode_kind} decode at relative index {relative_index}, start_data_complete_idx: {start_data_complete_idx}, end_data_complete_idx: {end_data_complete_idx}"
            );
        }
        DecodeEntriesError::Deserialize { payload_len, error } => {
            debug!(
                "Failed to deserialize bincode payload of size {} for slot {slot}, start_data_complete_idx: {start_data_complete_idx}, end_data_complete_idx: {end_data_complete_idx}, decode_kind: {decode_kind}. Err: {error}",
                payload_len,
            );
            metrics
                .bincode_deserialize_error_count
                .fetch_add(1, Ordering::Relaxed);
            if unknown_start {
                metrics
                    .unknown_start_position_error_count
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
        DecodeEntriesError::EntrySanity { entries_len } => {
            debug!(
                "Rejecting {decode_kind} decode with invalid entry hash chain for slot {slot}, start_data_complete_idx: {start_data_complete_idx}, end_data_complete_idx: {end_data_complete_idx}. entries_len: {}",
                entries_len,
            );
            metrics
                .entry_sanity_error_count
                .fetch_add(1, Ordering::Relaxed);
            if unknown_start {
                metrics
                    .unknown_start_position_error_count
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

fn build_known_start_scrub_candidate_indices(
    overlap_start: usize,
    overlap_end: usize,
    max_indices: usize,
    out: &mut Vec<usize>,
) {
    out.clear();
    if max_indices == 0 || overlap_start > overlap_end {
        return;
    }

    let span = overlap_end - overlap_start + 1;
    let target = max_indices.min(span);
    if target == 1 {
        out.push(overlap_start);
        return;
    }

    for i in 0..target {
        let idx = overlap_start + (i * (span - 1)) / (target - 1);
        if out.last().copied() != Some(idx) {
            out.push(idx);
        }
    }
    if out.len() >= target {
        return;
    }
    for idx in overlap_start..=overlap_end {
        if out.binary_search(&idx).is_ok() {
            continue;
        }
        out.push(idx);
        if out.len() >= target {
            break;
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn recover_data_for_fec_key(
    slot: Slot,
    key: &FecSetKey,
    fec_sets: &mut ahash::HashMap<FecSetKey, FecSetState>,
    state_tracker: &mut ShredsStateTracker,
    rs_cache: &ReedSolomonCache,
    metrics: &ShredMetrics,
    scratch: &mut ReconstructScratch,
    first_shred_observed_at: Instant,
    force_missing_data_index: Option<usize>,
    enforce_missing_data_gate: bool,
    respect_recovery_throttle: bool,
) -> RecoverFecOutcome {
    let mut outcome = RecoverFecOutcome::default();
    let fec_set_index = key.fec_set_index;
    if state_tracker.already_recovered_fec_sets[fec_set_index as usize] {
        return outcome;
    }

    let (
        num_expected_data_shreds,
        num_expected_coding_shreds,
        num_coding_shreds,
        mut num_data_shreds,
    ) = {
        let Some(fec_state) = fec_sets.get_mut(key) else {
            // No coding shreds tracked for this FEC set => can't recover.
            return outcome;
        };
        maybe_init_fec_data_count(fec_state, key, state_tracker);
        (
            fec_state.num_data_shreds,
            fec_state.num_coding_shreds,
            fec_state.coding_count,
            fec_state.data_count,
        )
    };
    if num_expected_data_shreds == 0 || num_coding_shreds == 0 {
        return outcome;
    }

    if enforce_missing_data_gate
        && has_all_data_shreds_for_fec_set(state_tracker, key, num_expected_data_shreds)
    {
        return outcome;
    }

    let start = fec_set_index as usize;
    let Some(end_excl) = start.checked_add(num_expected_data_shreds as usize) else {
        return outcome;
    };
    if end_excl > state_tracker.data_shreds.len() {
        return outcome;
    }

    let forced_missing_index = match force_missing_data_index {
        Some(idx)
            if idx >= start
                && idx < end_excl
                && state_tracker.data_shreds[idx]
                    .as_ref()
                    .is_some_and(|s| shred_matches_fec_key(s, key)) =>
        {
            Some(idx)
        }
        Some(_) => return outcome,
        None => None,
    };
    let forced_missing_count = usize::from(forced_missing_index.is_some());

    // Fast path gate: use incremental count to skip impossible recover attempts.
    let mut total_shards = (num_data_shreds as usize) + (num_coding_shreds as usize);

    let observed_data_shreds = (start..end_excl)
        .filter(|&idx| {
            state_tracker.data_shreds[idx]
                .as_ref()
                .is_some_and(|s| shred_matches_fec_key(s, key))
        })
        .count()
        .min(u16::MAX as usize) as u16;
    if observed_data_shreds != num_data_shreds {
        num_data_shreds = observed_data_shreds;
        if let Some(fec_state) = fec_sets.get_mut(key) {
            fec_state.data_count = observed_data_shreds;
            fec_state.data_count_initialized = true;
            cap_last_recover_shard_count(fec_state);
        }
        total_shards = (num_data_shreds as usize) + (num_coding_shreds as usize);
    }

    let effective_total_shards = total_shards.saturating_sub(forced_missing_count);
    if effective_total_shards < num_expected_data_shreds as usize {
        return outcome;
    }

    if respect_recovery_throttle {
        // Avoid re-running recovery when neither shard count nor shard contents changed.
        let total_shards_u16 = total_shards.min(u16::MAX as usize) as u16;
        let (last_recover_shard_count, last_recover_generation, generation) = fec_sets
            .get(key)
            .map(|fec_state| {
                (
                    fec_state.last_recover_shard_count,
                    fec_state.last_recover_generation,
                    fec_state.generation,
                )
            })
            .unwrap_or_default();
        if total_shards_u16 <= last_recover_shard_count && generation == last_recover_generation {
            return outcome;
        }
        if let Some(fec_state) = fec_sets.get_mut(key) {
            fec_state.last_recover_shard_count = total_shards_u16;
            fec_state.last_recover_generation = fec_state.generation;
        }
    }

    // `merkle::recover` internally sorts shreds by erasure shard index.
    let mut merkle_shreds = Vec::with_capacity(effective_total_shards);
    scratch.variant_profile_counts.clear();
    for idx in start..end_excl {
        if forced_missing_index == Some(idx) {
            continue;
        }
        if let Some(shred) = state_tracker.data_shreds[idx]
            .as_ref()
            .filter(|s| shred_matches_fec_key(s, key))
        {
            let (_tag, proof_size, chained, resigned) =
                merkle_variant_fields(shred.common_header().shred_variant);
            *scratch
                .variant_profile_counts
                .entry((proof_size, chained, resigned))
                .or_default() += 1;
            merkle_shreds.push(shred.clone());
        }
    }
    if let Some(fec_state) = fec_sets.get(key) {
        for shred in fec_state.coding_by_pos.iter().filter_map(|s| s.as_ref()) {
            let (_tag, proof_size, chained, resigned) =
                merkle_variant_fields(shred.common_header().shred_variant);
            *scratch
                .variant_profile_counts
                .entry((proof_size, chained, resigned))
                .or_default() += 1;
            merkle_shreds.push(shred.clone());
        }
    }
    let Some((&dominant_variant, _)) = scratch
        .variant_profile_counts
        .iter()
        .max_by(|(va, ca), (vb, cb)| ca.cmp(cb).then_with(|| va.cmp(vb)))
    else {
        return outcome;
    };
    let total_before_variant_filter = merkle_shreds.len();
    merkle_shreds.retain(|shred| {
        let (_tag, proof_size, chained, resigned) =
            merkle_variant_fields(shred.common_header().shred_variant);
        (proof_size, chained, resigned) == dominant_variant
    });
    let filtered_variant_shreds = total_before_variant_filter.saturating_sub(merkle_shreds.len());
    if filtered_variant_shreds > 0 {
        datapoint_warn!(
            "shredstream_proxy-deshred_recovery_variant_mismatch",
            ("slot", slot, i64),
            ("fec_set_index", fec_set_index, i64),
            ("proof_size_kept", dominant_variant.0, i64),
            ("chained_kept", dominant_variant.1, bool),
            ("resigned_kept", dominant_variant.2, bool),
            (
                "variant_profiles_seen",
                scratch.variant_profile_counts.len(),
                i64
            ),
            ("variant_shreds_filtered", filtered_variant_shreds, i64),
            (
                "total_shards_before_filter",
                total_before_variant_filter,
                i64
            ),
            ("total_shards_after_filter", merkle_shreds.len(), i64),
            ("num_expected_data_shreds", num_expected_data_shreds, i64),
        );
    }
    if merkle_shreds.len() < num_expected_data_shreds as usize {
        // Even after removing inconsistent-variant shards we still don't have enough
        // shards to recover data in this FEC set.
        metrics
            .fec_recovery_error_count
            .fetch_add(1, Ordering::Relaxed);
        outcome.recover_failed = true;
        return outcome;
    }

    let recovered = match solana_ledger::shred::merkle::recover(merkle_shreds, rs_cache) {
        Ok(r) => r, // recovered shreds (data first, then code)
        Err(e) => {
            warn!(
                "Failed to recover shreds for slot {slot} fec_set_index {fec_set_index}. num_expected_data_shreds: {num_expected_data_shreds}, num_data_shreds: {num_data_shreds} num_expected_coding_shreds: {num_expected_coding_shreds} num_coding_shreds: {num_coding_shreds} force_missing_data_index: {:?} Err: {e}",
                forced_missing_index,
            );
            if matches!(e, solana_ledger::shred::Error::InvalidMerkleRoot) {
                metrics
                    .fec_recovery_invalid_merkle_root_count
                    .fetch_add(1, Ordering::Relaxed);
            }
            metrics
                .fec_recovery_error_count
                .fetch_add(1, Ordering::Relaxed);
            outcome.recover_failed = true;
            return outcome;
        }
    };

    for shred in recovered {
        match shred {
            Ok(shred) => match &shred {
                Shred::ShredData(_) => {
                    let index = shred.index() as usize;
                    let old_key = state_tracker.data_shreds[index]
                        .as_ref()
                        .map(FecSetKey::from_shred);
                    if ingest_data_shred(shred, state_tracker, DataShredSource::Recovered) {
                        let new_key = state_tracker.data_shreds[index]
                            .as_ref()
                            .map(FecSetKey::from_shred);
                        update_fec_state_after_data_shred_change(fec_sets, old_key, new_key);
                        if let Some(new_key) = new_key {
                            mark_fec_set_first_shred_seen(
                                fec_sets,
                                new_key,
                                first_shred_observed_at,
                            );
                        }
                        outcome.recovered_data_shreds += 1;
                    }
                }
                Shred::ShredCode(_) => {
                    // Keep recovered coding shreds so future recover attempts have a richer set.
                    let recovered_key = FecSetKey::from_shred(&shred);
                    let inserted = {
                        let fec_state = fec_sets.entry(recovered_key).or_default();
                        let inserted = fec_state.insert_coding_shred(shred);
                        if inserted {
                            maybe_init_fec_data_count(fec_state, &recovered_key, state_tracker);
                        }
                        inserted
                    };
                    if inserted {
                        mark_fec_set_first_shred_seen(
                            fec_sets,
                            recovered_key,
                            first_shred_observed_at,
                        );
                    }
                }
            },
            Err(e) => warn!(
                "Failed to recover shred for slot {slot}, fec set: {fec_set_index}. force_missing_data_index: {:?}. Err: {e}",
                forced_missing_index,
            ),
        }
    }

    outcome
}

#[allow(clippy::too_many_arguments)]
fn try_known_start_parity_scrub(
    slot: Slot,
    start_data_complete_idx: ShredIndex,
    end_data_complete_idx: ShredIndex,
    fec_sets: &mut ahash::HashMap<FecSetKey, FecSetState>,
    state_tracker: &mut ShredsStateTracker,
    rs_cache: &ReedSolomonCache,
    cfg: ReconstructShredsConfig,
    metrics: &ShredMetrics,
    scratch: &mut ReconstructScratch,
    first_shred_observed_at: Instant,
    total_recovered_data_shreds: &mut usize,
) -> bool {
    if !cfg.known_start_parity_scrub_enabled
        || cfg.known_start_parity_scrub_max_fec_sets_per_failure == 0
        || cfg.known_start_parity_scrub_max_indices_per_fec == 0
        || cfg.known_start_parity_scrub_max_attempts_per_fec_generation == 0
    {
        return false;
    }

    let range_start = start_data_complete_idx as usize;
    let range_end = end_data_complete_idx as usize;

    scratch.known_start_scrub_fec_keys.clear();
    for idx in range_start..=range_end {
        if let Some(shred) = state_tracker.data_shreds[idx].as_ref() {
            scratch
                .known_start_scrub_fec_keys
                .push(FecSetKey::from_shred(shred));
        }
    }
    scratch.known_start_scrub_fec_keys.sort_unstable();
    scratch.known_start_scrub_fec_keys.dedup();

    let mut fec_sets_considered = 0usize;
    for key_idx in 0..scratch.known_start_scrub_fec_keys.len() {
        if fec_sets_considered >= cfg.known_start_parity_scrub_max_fec_sets_per_failure as usize {
            break;
        }
        let key = scratch.known_start_scrub_fec_keys[key_idx];
        let Some((overlap_start, overlap_end)) = (|| {
            let fec_state = fec_sets.get_mut(&key)?;
            maybe_init_fec_data_count(fec_state, &key, state_tracker);
            if fec_state.num_data_shreds == 0 || fec_state.coding_count == 0 {
                return None;
            }
            if !has_all_data_shreds_for_fec_set(state_tracker, &key, fec_state.num_data_shreds) {
                return None;
            }

            let fec_start = key.fec_set_index as usize;
            let fec_end = fec_start
                .checked_add(fec_state.num_data_shreds as usize)?
                .checked_sub(1)?;
            let overlap_start = fec_start.max(range_start);
            let overlap_end = fec_end.min(range_end);
            if overlap_start > overlap_end {
                return None;
            }
            Some((overlap_start, overlap_end))
        })() else {
            continue;
        };
        fec_sets_considered = fec_sets_considered.saturating_add(1);

        let candidate_budget = usize::from(cfg.known_start_parity_scrub_max_indices_per_fec)
            .min(cfg.known_start_parity_scrub_max_attempts_per_fec_generation as usize);
        build_known_start_scrub_candidate_indices(
            overlap_start,
            overlap_end,
            candidate_budget,
            &mut scratch.known_start_scrub_candidate_indices,
        );

        for candidate_idx in 0..scratch.known_start_scrub_candidate_indices.len() {
            let forced_missing_idx = scratch.known_start_scrub_candidate_indices[candidate_idx];
            let Some(fec_state) = fec_sets.get_mut(&key) else {
                break;
            };
            if fec_state.known_start_scrub_generation != fec_state.generation {
                fec_state.known_start_scrub_generation = fec_state.generation;
                fec_state.known_start_scrub_attempts_in_generation = 0;
            }
            if fec_state.known_start_scrub_attempts_in_generation
                >= cfg.known_start_parity_scrub_max_attempts_per_fec_generation
            {
                metrics
                    .known_start_parity_scrub_skip_budget_count
                    .fetch_add(1, Ordering::Relaxed);
                break;
            }
            fec_state.known_start_scrub_attempts_in_generation += 1;
            metrics
                .known_start_parity_scrub_attempt_count
                .fetch_add(1, Ordering::Relaxed);
            let outcome = recover_data_for_fec_key(
                slot,
                &key,
                fec_sets,
                state_tracker,
                rs_cache,
                metrics,
                scratch,
                first_shred_observed_at,
                Some(forced_missing_idx),
                false,
                false,
            );
            if outcome.recover_failed {
                metrics
                    .known_start_parity_scrub_error_count
                    .fetch_add(1, Ordering::Relaxed);
            }
            if outcome.recovered_data_shreds > 0 {
                metrics
                    .known_start_parity_scrub_success_count
                    .fetch_add(1, Ordering::Relaxed);
                *total_recovered_data_shreds =
                    total_recovered_data_shreds.saturating_add(outcome.recovered_data_shreds);
                return true;
            }
        }
    }

    false
}

/// Reconstruct missing data shreds, deshred completed ranges, and emit decoded entry payloads.
///
/// Returns the number of recovered **data** shreds in this invocation.
/// Accepts multiple packet batches so ingestion can drain queued traffic before recovery.
#[allow(clippy::too_many_arguments)]
pub(crate) fn reconstruct_shreds(
    packet_batches: Vec<PacketBatch>,
    all_shreds: &mut ahash::HashMap<
        Slot,
        (ahash::HashMap<FecSetKey, FecSetState>, ShredsStateTracker),
    >,
    slot_fec_keys_to_iterate: &mut Vec<(Slot, FecSetKey)>,
    deshredded_entries: &mut Vec<(Slot, Vec<solana_entry::entry::Entry>, Vec<u8>)>,
    highest_slot_seen: &mut Slot,
    rs_cache: &ReedSolomonCache,
    cfg: ReconstructShredsConfig,
    metrics: &ShredMetrics,
    scratch: &mut ReconstructScratch,
) -> usize {
    deshredded_entries.clear();
    slot_fec_keys_to_iterate.clear();
    let total_packets: usize = packet_batches.iter().map(|b| b.len()).sum();
    if scratch.parsed_shreds.capacity() < total_packets {
        scratch
            .parsed_shreds
            .reserve(total_packets - scratch.parsed_shreds.capacity());
    }
    if scratch.slot_samples.capacity() < total_packets {
        scratch
            .slot_samples
            .reserve(total_packets - scratch.slot_samples.capacity());
    }
    scratch.parsed_shreds.clear();
    scratch.slot_samples.clear();
    scratch.data_complete_idxs.clear();
    scratch.unknown_start_candidates.clear();
    scratch.unknown_start_missing_suffix.clear();
    scratch.variant_profile_counts.clear();
    // Phase 1: Ingest packets from ALL batches.
    //
    // Compute a combined "slot anchor" as the median parsed slot to avoid being driven by a
    // single outlier slot (slot poisoning). Then only accept shreds within a bounded window
    // around that anchor.
    //
    // Pass A: sample versions and pick the dominant shred version for ingress filtering.
    scratch.version_counts.clear();
    for packet in packet_batches
        .iter()
        .flat_map(|b| b.iter())
        .filter(|p| !p.meta().discard())
    {
        let Some(packet_bytes) = packet.data(..) else {
            continue;
        };
        if packet_bytes.len() <= solana_ledger::shred::SIZE_OF_SIGNATURE {
            continue;
        }
        let Some(expected_len) =
            merkle_payload_size_from_tag(packet_bytes[solana_ledger::shred::SIZE_OF_SIGNATURE])
        else {
            continue;
        };
        if packet_bytes.len() < expected_len {
            // Do not let truncated packets influence dominant-version selection.
            continue;
        }
        let Some(version_bytes) = packet_bytes.get(77..79) else {
            continue;
        };
        let version = u16::from_le_bytes([version_bytes[0], version_bytes[1]]);
        *scratch.version_counts.entry(version).or_default() += 1;
    }
    // Deterministic tie-break by version value.
    let expected_shred_version = scratch
        .version_counts
        .iter()
        .max_by(|(va, ca), (vb, cb)| ca.cmp(cb).then_with(|| va.cmp(vb)))
        .map(|(version, _count)| *version);
    // Pass B: apply ingress filtering + parse shreds.
    let mut shred_fetch_stats = solana_ledger::shred::ShredFetchStats::default();
    for packet in packet_batches
        .iter()
        .flat_map(|b| b.iter())
        .filter(|p| !p.meta().discard())
    {
        if expected_shred_version.is_some_and(|expected_version| {
            solana_ledger::shred::should_discard_shred(
                packet,
                0, // root
                Slot::MAX,
                expected_version,
                |_| false, // keep unchained Merkle shreds for compatibility
                &mut shred_fetch_stats,
            )
        }) {
            metrics
                .reconstruct_ingress_filter_drop_count
                .fetch_add(1, Ordering::Relaxed);
            continue;
        }
        let Some(packet) = packet.data(..) else {
            continue;
        };
        // Ignore trailing bytes beyond the canonical shred payload size (spec §3.1).
        let packet_prefix = if packet.len() <= solana_ledger::shred::SIZE_OF_SIGNATURE {
            packet
        } else {
            match merkle_payload_size_from_tag(packet[solana_ledger::shred::SIZE_OF_SIGNATURE]) {
                Some(expected_len) if packet.len() >= expected_len => &packet[..expected_len],
                _ => packet,
            }
        };
        // Wrap bytes in shared payload so later shred clones are cheap during recovery.
        let payload = solana_ledger::shred::Payload::from(Arc::new(packet_prefix.to_vec()));
        match solana_ledger::shred::Shred::new_from_serialized_shred(payload)
            .and_then(Shred::try_from)
        {
            Ok(shred) => {
                let slot = shred.common_header().slot;
                let index = shred.index() as usize;
                let fec_set_index = shred.fec_set_index();
                let key = FecSetKey::from_shred(&shred);
                if index >= MAX_DATA_SHREDS_PER_SLOT
                    || (fec_set_index as usize) >= MAX_DATA_SHREDS_PER_SLOT
                {
                    debug!(
                        "Out-of-bounds shred slot: {slot}, fec_set_index: {fec_set_index}, index: {index}"
                    );
                    continue;
                }
                let index = index as ShredIndex;
                scratch.slot_samples.push(slot);
                scratch.parsed_shreds.push((shred, slot, index, key));
            }
            Err(e) => {
                if TraceShred::decode(packet).is_ok() {
                    continue;
                }
                warn!("Failed to decode shred. Err: {e:?}");
            }
        }
    }

    let slot_anchor = if scratch.slot_samples.is_empty() {
        *highest_slot_seen
    } else {
        let mid = scratch.slot_samples.len() / 2;
        let (_, anchor, _) = scratch.slot_samples.select_nth_unstable(mid);
        *anchor
    };
    *highest_slot_seen = (*highest_slot_seen).max(slot_anchor);
    let min_slot = slot_anchor.saturating_sub(cfg.slot_lookback);
    let max_slot = slot_anchor.saturating_add(cfg.slot_future);

    let first_shred_observed_at = Instant::now();
    for (shred, slot, index, key) in scratch.parsed_shreds.drain(..) {
        let index = index as usize;
        let fec_set_index = key.fec_set_index;
        if slot < min_slot {
            debug!(
                "Old shred slot: {slot}, slot_anchor: {slot_anchor}, fec_set_index: {fec_set_index}, index: {index}"
            );
            metrics
                .reconstruct_slot_window_drop_old_count
                .fetch_add(1, Ordering::Relaxed);
            continue;
        }
        if slot > max_slot {
            debug!(
                "Future shred slot: {slot}, slot_anchor: {slot_anchor}, fec_set_index: {fec_set_index}, index: {index}"
            );
            metrics
                .reconstruct_slot_window_drop_future_count
                .fetch_add(1, Ordering::Relaxed);
            continue;
        }

        let (fec_sets, state_tracker) = all_shreds.entry(slot).or_default();
        // Skip any shreds belonging to a fully-recovered FEC set.
        if state_tracker.already_recovered_fec_sets[fec_set_index as usize] {
            continue;
        }
        let inserted = match shred {
            Shred::ShredData(_) => {
                let old_key = state_tracker.data_shreds[index]
                    .as_ref()
                    .map(FecSetKey::from_shred);
                let inserted = ingest_data_shred(shred, state_tracker, DataShredSource::Incoming);
                if inserted {
                    let new_key = state_tracker.data_shreds[index]
                        .as_ref()
                        .map(FecSetKey::from_shred);
                    update_fec_state_after_data_shred_change(fec_sets, old_key, new_key);
                    if let Some(new_key) = new_key {
                        mark_fec_set_first_shred_seen(fec_sets, new_key, first_shred_observed_at);
                    }
                }
                inserted
            }
            Shred::ShredCode(_) => {
                let inserted = {
                    let fec_state = fec_sets.entry(key).or_default();
                    let inserted = fec_state.insert_coding_shred(shred);
                    if inserted {
                        maybe_init_fec_data_count(fec_state, &key, state_tracker);
                    }
                    inserted
                };
                if inserted {
                    mark_fec_set_first_shred_seen(fec_sets, key, first_shred_observed_at);
                }
                inserted
            }
        };
        if !inserted {
            continue;
        }
        // Use Vec so we can sort to make sure if any earlier FEC sets have DATA_COMPLETE_SHRED,
        // later entries can use the flag to find the bounds.
        slot_fec_keys_to_iterate.push((slot, key));
    }
    // Deterministic processing order prevents boundary-dependent decode jitter.
    slot_fec_keys_to_iterate.sort_unstable();
    slot_fec_keys_to_iterate.dedup();

    // Phase 2: Try recovering by FEC set.
    // Note: `merkle::recover` can return both recovered data and coding shreds; we only
    // care about recovered *data* shreds for decoding entries.
    let mut total_recovered_data_shreds = 0usize;
    for (slot, key) in slot_fec_keys_to_iterate.iter() {
        let (fec_sets, state_tracker) = all_shreds.entry(*slot).or_default();
        let outcome = recover_data_for_fec_key(
            *slot,
            key,
            fec_sets,
            state_tracker,
            rs_cache,
            metrics,
            scratch,
            first_shred_observed_at,
            None,
            true,
            true,
        );
        total_recovered_data_shreds =
            total_recovered_data_shreds.saturating_add(outcome.recovered_data_shreds);
    }

    // Phase 3: Deshred and bincode deserialize any completed data sets.
    //
    // We treat `DATA_COMPLETE_SHRED` indices as end-boundaries. For each boundary, we find a
    // start index:
    // - known start: right after the previous `DATA_COMPLETE_SHRED` (or 0 if none).
    // - unknown start: right after a gap (missing shred) that we haven't already consumed.
    //
    // Unknown-start decodes are best-effort: we emit entries but keep payloads so a later correct
    // decode (once the true boundary shred arrives) can supersede it.
    for slot in slot_fec_keys_to_iterate
        .iter()
        .map(|(slot, _fec_key)| *slot)
        .dedup()
    {
        let (fec_sets, state_tracker) = all_shreds.entry(slot).or_default();

        // Snapshot end boundaries so we can mutate tracker while iterating.
        scratch.data_complete_idxs.clear();
        scratch.data_complete_idxs.extend(
            state_tracker
                .data_complete
                .iter_ones()
                .map(|idx| idx as ShredIndex),
        );
        for end_idx_pos in 0..scratch.data_complete_idxs.len() {
            let end_data_complete_idx = scratch.data_complete_idxs[end_idx_pos];
            let end_data_complete_idx_usize = end_data_complete_idx as usize;
            if state_tracker.already_deshredded[end_data_complete_idx_usize] {
                continue;
            }

            // Find the primary start boundary.
            let (initial_start_data_complete_idx, initial_unknown_start) = {
                let mut start = end_data_complete_idx;
                let mut unknown = false;
                let mut i = end_data_complete_idx_usize;
                while i > 0 {
                    let prev = i - 1;
                    if state_tracker.data_complete.get(prev).is_some_and(|b| *b) {
                        start = i as ShredIndex;
                        break;
                    }
                    // Treat missing shreds as a boundary only if they haven't already been
                    // consumed (we may have dropped payloads for already-deshredded indices).
                    if state_tracker.data_shreds[prev].is_none()
                        && !state_tracker.already_deshredded[prev]
                    {
                        start = i as ShredIndex;
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
            let initial_range_generation = state_tracker
                .range_generation_max(initial_start_data_complete_idx, end_data_complete_idx);
            let emitted_unknown_start = state_tracker
                .unknown_start_emitted
                .get(&end_data_complete_idx)
                .copied();
            let emitted_start =
                emitted_unknown_start.map(|emitted| emitted.start_data_complete_idx);
            let emitted_range_generation =
                emitted_unknown_start.map(|emitted| emitted.range_generation);
            if initial_unknown_start && emitted_start.is_some() {
                // We already emitted a best-effort unknown-start decode for this end boundary.
                // Do not re-emit until a known boundary becomes available.
                continue;
            }
            if initial_unknown_start
                && state_tracker
                    .unknown_start_last_attempt_generation
                    .get(&end_data_complete_idx)
                    .is_some_and(|generation| *generation == initial_range_generation)
            {
                // No data payload changed in this candidate range since the last failed unknown-start
                // attempt. Skip speculative rework.
                continue;
            }
            scratch.unknown_start_candidates.clear();
            let mut end_dc_fec_start: Option<ShredIndex> = None;
            if initial_unknown_start {
                // The backward scan may have crossed into an incomplete adjacent
                // FEC set, landing in the middle of a different serialized
                // Vec<Entry>. On mainnet every FEC set ends with
                // DATA_COMPLETE_SHRED at its last data position, so each FEC set
                // is a self-contained data set. Trying the fec_set_index of the
                // end DC shred first gives us the true data-set boundary and
                // avoids the bincode deserialization failures that occur when the
                // range begins mid-data-set.
                if let Some(dc_shred) = &state_tracker.data_shreds[end_data_complete_idx_usize] {
                    let fec_start = dc_shred.fec_set_index() as ShredIndex;
                    if fec_start > initial_start_data_complete_idx
                        && fec_start <= end_data_complete_idx
                    {
                        end_dc_fec_start = Some(fec_start);
                        scratch.unknown_start_candidates.push(fec_start);
                    }
                }
                for idx in initial_start_data_complete_idx
                    ..=initial_start_data_complete_idx
                        .saturating_add(cfg.unknown_start_max_positions.max(1) - 1)
                        .min(end_data_complete_idx)
                {
                    if !scratch.unknown_start_candidates.contains(&idx) {
                        scratch.unknown_start_candidates.push(idx);
                    }
                }

                // Rank unknown-start candidates by:
                // 1) fewer missing shards in [candidate, end] (decode-feasibility signal),
                // 2) stronger boundary signal (end-DC FEC start first, then generic FEC starts),
                // 3) earlier index for maximal entry coverage when ties remain.
                let window_start = initial_start_data_complete_idx as usize;
                let window_end = end_data_complete_idx_usize;
                let window_len = window_end.saturating_sub(window_start).saturating_add(1);
                scratch.unknown_start_missing_suffix.clear();
                if scratch.unknown_start_missing_suffix.capacity() < window_len {
                    scratch
                        .unknown_start_missing_suffix
                        .reserve(window_len - scratch.unknown_start_missing_suffix.capacity());
                }
                scratch
                    .unknown_start_missing_suffix
                    .resize(window_len, 0u16);
                for offset in (0..window_len).rev() {
                    let idx = window_start + offset;
                    let missing_here = (state_tracker.already_deshredded[idx]
                        || state_tracker.data_shreds[idx].is_none())
                        as u16;
                    let next_missing = if offset + 1 < window_len {
                        scratch.unknown_start_missing_suffix[offset + 1]
                    } else {
                        0
                    };
                    scratch.unknown_start_missing_suffix[offset] =
                        next_missing.saturating_add(missing_here);
                }
                scratch
                    .unknown_start_candidates
                    .sort_unstable_by_key(|candidate| {
                        let candidate_usize = *candidate as usize;
                        let missing_suffix = candidate_usize
                            .checked_sub(window_start)
                            .and_then(|i| scratch.unknown_start_missing_suffix.get(i).copied())
                            .unwrap_or(u16::MAX);
                        let boundary_rank = if Some(*candidate) == end_dc_fec_start {
                            0u8
                        } else if state_tracker.data_shreds[candidate_usize]
                            .as_ref()
                            .is_some_and(|s| s.fec_set_index() as ShredIndex == *candidate)
                        {
                            1u8
                        } else {
                            2u8
                        };
                        (missing_suffix, boundary_rank, *candidate)
                    });
            } else {
                scratch
                    .unknown_start_candidates
                    .push(initial_start_data_complete_idx);
            }
            let mut unknown_start_succeeded = false;

            'candidate_start: for candidate_idx in 0..scratch.unknown_start_candidates.len() {
                let start_data_complete_idx = scratch.unknown_start_candidates[candidate_idx];
                if initial_unknown_start {
                    metrics
                        .unknown_start_position_count
                        .fetch_add(1, Ordering::Relaxed);
                }

                // Require all shreds in the candidate range.
                let start_data_complete_idx_usize = start_data_complete_idx as usize;
                if (start_data_complete_idx_usize..=end_data_complete_idx_usize).any(|idx| {
                    state_tracker.already_deshredded[idx]
                        || state_tracker.data_shreds[idx].is_none()
                }) {
                    continue;
                }

                if !initial_unknown_start
                    && emitted_start == Some(start_data_complete_idx)
                    && emitted_range_generation
                        == Some(
                            state_tracker.range_generation_max(
                                start_data_complete_idx,
                                end_data_complete_idx,
                            ),
                        )
                {
                    // We already emitted this exact range as an unknown-start decode. Now that the
                    // true boundary is present (known start), commit without re-emitting.
                    // If slot data changed after the provisional emit, we must re-decode.
                    observe_fec_set_decode_completion_latency(
                        fec_sets,
                        metrics,
                        &state_tracker.data_shreds
                            [start_data_complete_idx_usize..=end_data_complete_idx_usize],
                        DecodeCompletionKind::KnownStart,
                    );
                    commit_deshredded_range(
                        fec_sets,
                        state_tracker,
                        start_data_complete_idx,
                        end_data_complete_idx,
                    );
                    state_tracker
                        .unknown_start_emitted
                        .remove(&end_data_complete_idx);
                    break;
                }
                let mut scrub_retry_used = false;
                let (entries, deshredded_payload) = 'decode_attempt: loop {
                    match decode_entries_from_shred_range(
                        &state_tracker.data_shreds
                            [start_data_complete_idx_usize..=end_data_complete_idx_usize],
                        initial_unknown_start,
                    ) {
                        Ok(decoded) => break 'decode_attempt decoded,
                        Err(err) => {
                            if initial_unknown_start {
                                record_decode_entries_error(
                                    metrics,
                                    slot,
                                    start_data_complete_idx,
                                    end_data_complete_idx,
                                    true,
                                    &err,
                                );
                                continue 'candidate_start;
                            }

                            if !scrub_retry_used
                                && try_known_start_parity_scrub(
                                    slot,
                                    start_data_complete_idx,
                                    end_data_complete_idx,
                                    fec_sets,
                                    state_tracker,
                                    rs_cache,
                                    cfg,
                                    metrics,
                                    scratch,
                                    first_shred_observed_at,
                                    &mut total_recovered_data_shreds,
                                )
                            {
                                scrub_retry_used = true;
                                continue 'decode_attempt;
                            }

                            record_decode_entries_error(
                                metrics,
                                slot,
                                start_data_complete_idx,
                                end_data_complete_idx,
                                false,
                                &err,
                            );
                            continue 'candidate_start;
                        }
                    }
                };

                if scrub_retry_used {
                    metrics
                        .known_start_parity_scrub_decode_retry_success_count
                        .fetch_add(1, Ordering::Relaxed);
                }

                metrics
                    .entry_count
                    .fetch_add(entries.len() as u64, Ordering::Relaxed);
                metrics.txn_count.fetch_add(
                    entries.iter().map(|e| e.transactions.len() as u64).sum(),
                    Ordering::Relaxed,
                );
                debug!(
                    "Successfully decoded slot: {slot} start_data_complete_idx: {start_data_complete_idx} end_data_complete_idx: {end_data_complete_idx} with entry count: {}",
                    entries.len(),
                );

                metrics.deshred_set_count.fetch_add(1, Ordering::Relaxed);
                metrics
                    .deshred_bytes_count
                    .fetch_add(deshredded_payload.len() as u64, Ordering::Relaxed);
                deshredded_entries.push((slot, entries, deshredded_payload));

                if initial_unknown_start {
                    // Best-effort decode: emit, but do NOT drop payloads or mark the indices as
                    // deshredded. This avoids "locking in" a potentially wrong boundary and allows a
                    // later correct decode if/when the missing boundary shred arrives.
                    observe_fec_set_decode_completion_latency(
                        fec_sets,
                        metrics,
                        &state_tracker.data_shreds
                            [start_data_complete_idx_usize..=end_data_complete_idx_usize],
                        DecodeCompletionKind::UnknownStart,
                    );
                    state_tracker.unknown_start_emitted.insert(
                        end_data_complete_idx,
                        UnknownStartEmission {
                            start_data_complete_idx,
                            range_generation: state_tracker.range_generation_max(
                                start_data_complete_idx,
                                end_data_complete_idx,
                            ),
                        },
                    );
                    unknown_start_succeeded = true;
                    break;
                }

                // Known start: mark as consumed.
                observe_fec_set_decode_completion_latency(
                    fec_sets,
                    metrics,
                    &state_tracker.data_shreds
                        [start_data_complete_idx_usize..=end_data_complete_idx_usize],
                    DecodeCompletionKind::KnownStart,
                );
                commit_deshredded_range(
                    fec_sets,
                    state_tracker,
                    start_data_complete_idx,
                    end_data_complete_idx,
                );
                state_tracker
                    .unknown_start_emitted
                    .remove(&end_data_complete_idx);
                break;
            }
            if !initial_unknown_start || unknown_start_succeeded {
                state_tracker
                    .unknown_start_last_attempt_generation
                    .remove(&end_data_complete_idx);
            } else {
                state_tracker
                    .unknown_start_last_attempt_generation
                    .insert(end_data_complete_idx, initial_range_generation);
            }
        }
    }

    // Phase 4: Opportunistic eviction.
    //
    // `MAX_PROCESSING_AGE` is used here as a hard cap on the number of slots we keep in
    // `all_shreds` before running a retain-by-window pass. The window itself is bounded by
    // `[highest_slot_seen - slot_lookback, highest_slot_seen + slot_future]`.
    if all_shreds.len() > MAX_PROCESSING_AGE {
        let min_slot = highest_slot_seen.saturating_sub(cfg.slot_lookback);
        let max_slot = highest_slot_seen.saturating_add(cfg.slot_future);
        let mut incomplete_fec_sets = ahash::HashMap::<Slot, Vec<_>>::default();
        let mut incomplete_fec_sets_count = 0;
        all_shreds.retain(|slot, (fec_set_indexes, state_tracker)| {
            if *slot >= min_slot && *slot <= max_slot {
                return true;
            }
            let mut unknown_start_only_count = 0u64;
            let mut known_start_only_count = 0u64;
            let mut both_start_modes_count = 0u64;
            for fec_state in fec_set_indexes.values() {
                match (
                    fec_state.unknown_start_decode_completed,
                    fec_state.known_start_decode_completed,
                ) {
                    (true, false) => {
                        unknown_start_only_count = unknown_start_only_count.saturating_add(1)
                    }
                    (false, true) => {
                        known_start_only_count = known_start_only_count.saturating_add(1)
                    }
                    (true, true) => {
                        both_start_modes_count = both_start_modes_count.saturating_add(1)
                    }
                    (false, false) => {}
                }
            }
            if unknown_start_only_count > 0 {
                metrics
                    .fec_set_decode_unknown_start_only_count
                    .fetch_add(unknown_start_only_count, Ordering::Relaxed);
            }
            if known_start_only_count > 0 {
                metrics
                    .fec_set_decode_known_start_only_count
                    .fetch_add(known_start_only_count, Ordering::Relaxed);
            }
            if both_start_modes_count > 0 {
                metrics
                    .fec_set_decode_both_start_modes_count
                    .fetch_add(both_start_modes_count, Ordering::Relaxed);
            }

            // count missing fec sets before clearing
            for (fec_set_index, fec_state) in fec_set_indexes.iter() {
                if state_tracker.already_recovered_fec_sets[fec_set_index.fec_set_index as usize] {
                    continue;
                }
                let num_expected_data_shreds = fec_state.num_data_shreds;
                let mut shards_present = fec_state.coding_count as usize;
                if num_expected_data_shreds > 0 {
                    let start = fec_set_index.fec_set_index as usize;
                    if let Some(end_excl) = start
                        .checked_add(num_expected_data_shreds as usize)
                        .filter(|&end_excl| end_excl <= state_tracker.data_shreds.len())
                    {
                        shards_present += (start..end_excl)
                            .filter(|&idx| {
                                state_tracker.data_shreds[idx].is_some()
                                    || state_tracker.already_deshredded[idx]
                            })
                            .count();
                    }
                }

                incomplete_fec_sets_count += 1;
                incomplete_fec_sets
                    .entry(*slot)
                    .or_default()
                    .push((
                        fec_set_index.fec_set_index,
                        num_expected_data_shreds,
                        shards_present,
                    ));
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

/// Bump per-FEC generation when data/coding membership or payload changes.
#[inline]
fn bump_fec_state_generation(
    fec_sets: &mut ahash::HashMap<FecSetKey, FecSetState>,
    key: FecSetKey,
) {
    if let Some(state) = fec_sets.get_mut(&key) {
        state.generation = state.generation.wrapping_add(1);
    }
}

/// Insert a new **data** shred into the per-slot tracker.
/// Returns true on first-seen insert or when replacing a conflicting duplicate.
fn ingest_data_shred(
    shred: Shred,
    state_tracker: &mut ShredsStateTracker,
    source: DataShredSource,
) -> bool {
    let index = shred.index() as usize;
    let fec_set_index = shred.fec_set_index() as usize;
    if index >= state_tracker.data_shreds.len()
        || fec_set_index >= state_tracker.already_recovered_fec_sets.len()
    {
        return false;
    }
    if state_tracker.already_recovered_fec_sets[fec_set_index]
        || state_tracker.already_deshredded[index]
    {
        return false;
    }
    let Shred::ShredData(s) = &shred else {
        return false;
    };
    if let Some(existing) = state_tracker.data_shreds[index].as_ref() {
        let mut should_overwrite_with_new = false;
        match existing {
            Shred::ShredData(old) => {
                let payload_len_old = old.payload.len();
                let payload_len_new = s.payload.len();
                let sig_diff = count_byte_diffs(
                    old.common_header.signature.as_ref(),
                    s.common_header.signature.as_ref(),
                );
                let (old_tag, old_proof, old_chained, old_resigned) =
                    merkle_variant_fields(old.common_header.shred_variant);
                let (new_tag, new_proof, new_chained, new_resigned) =
                    merkle_variant_fields(s.common_header.shred_variant);

                let flags_old = old.data_header.flags.bits();
                let flags_new = s.data_header.flags.bits();
                let size_old = old.data_header.size;
                let size_new = s.data_header.size;
                let data_len_old = size_old.saturating_sub(88);
                let data_len_new = size_new.saturating_sub(88);
                let old_payload =
                    payload_compare_prefix(old.payload.as_ref(), old.common_header.shred_variant);
                let new_payload =
                    payload_compare_prefix(s.payload.as_ref(), s.common_header.shred_variant);
                let payload_byte_diff = count_byte_diffs(old_payload, new_payload);

                let is_conflict = sig_diff != 0
                    || old.common_header.version != s.common_header.version
                    || old.common_header.fec_set_index != s.common_header.fec_set_index
                    || old.common_header.index != s.common_header.index
                    || old_tag != new_tag
                    || old_proof != new_proof
                    || old_chained != new_chained
                    || old_resigned != new_resigned
                    || old.data_header.parent_offset != s.data_header.parent_offset
                    || flags_old != flags_new
                    || size_old != size_new
                    || payload_len_old != payload_len_new
                    || payload_byte_diff != 0;

                if is_conflict {
                    should_overwrite_with_new = true;
                    datapoint_info!(
                        "shredstream_proxy-deshred_shred_conflict",
                        "kind" => "data",
                        "reason" => "duplicate_index",
                        ("slot", s.common_header.slot, i64),
                        ("index", s.common_header.index, i64),
                        ("payload_len_old", payload_len_old, i64),
                        ("payload_len_new", payload_len_new, i64),
                        ("payload_len_diff", payload_len_new.abs_diff(payload_len_old), i64),
                        ("payload_diff_bytes", payload_byte_diff, i64),
                        ("version_old", old.common_header.version, i64),
                        ("version_new", s.common_header.version, i64),
                        ("sig_diff_bytes", sig_diff, i64),
                        ("fec_set_index_old", old.common_header.fec_set_index, i64),
                        ("fec_set_index_new", s.common_header.fec_set_index, i64),
                        ("variant_tag_old", old_tag, i64),
                        ("variant_tag_new", new_tag, i64),
                        ("proof_size_old", old_proof, i64),
                        ("proof_size_new", new_proof, i64),
                        ("chained_old", old_chained, bool),
                        ("chained_new", new_chained, bool),
                        ("resigned_old", old_resigned, bool),
                        ("resigned_new", new_resigned, bool),
                        ("flags_old", flags_old, i64),
                        ("flags_new", flags_new, i64),
                        ("flags_xor", flags_old ^ flags_new, i64),
                        ("size_old", size_old, i64),
                        ("size_new", size_new, i64),
                        ("size_diff", size_new.abs_diff(size_old), i64),
                        ("data_len_old", data_len_old, i64),
                        ("data_len_new", data_len_new, i64),
                        ("data_len_diff", data_len_new.abs_diff(data_len_old), i64),
                        ("parent_offset_old", old.data_header.parent_offset, i64),
                        ("parent_offset_new", s.data_header.parent_offset, i64),
                    );
                }
            }
            Shred::ShredCode(old) => {
                // This should never happen: data_shreds[] should only contain data shreds.
                should_overwrite_with_new = true;
                datapoint_info!(
                    "shredstream_proxy-deshred_shred_conflict",
                    "kind" => "data",
                    "reason" => "type_mismatch_existing_code",
                    ("slot", s.common_header.slot, i64),
                    ("index", s.common_header.index, i64),
                    ("existing_payload_len", old.payload.len(), i64),
                    ("new_payload_len", s.payload.len(), i64),
                );
            }
        }
        if should_overwrite_with_new {
            let was_complete = state_tracker.data_complete[index];
            let now_complete = s.data_complete() || s.last_in_slot();
            let slot = s.common_header.slot;
            let shred_index = s.common_header.index;
            if was_complete && !now_complete {
                state_tracker
                    .unknown_start_emitted
                    .remove(&(index as ShredIndex));
                state_tracker
                    .unknown_start_last_attempt_generation
                    .remove(&(index as ShredIndex));
            }
            state_tracker.data_complete.set(index, now_complete);
            datapoint_info!(
                "shredstream_proxy-deshred_shred_conflict",
                "kind" => "data",
                "reason" => "overwrite_duplicate_index",
                "source" => match source {
                    DataShredSource::Incoming => "incoming",
                    DataShredSource::Recovered => "recovered",
                },
                ("slot", slot, i64),
                ("index", shred_index, i64),
            );
            store_data_shred_at_index(state_tracker, index, now_complete, shred);
            return true;
        }
        return false;
    }

    let is_data_complete = s.data_complete() || s.last_in_slot();
    store_data_shred_at_index(state_tracker, index, is_data_complete, shred);
    true
}

/// Return true when every expected data index in the set is present or already consumed.
fn has_all_data_shreds_for_fec_set(
    tracker: &ShredsStateTracker,
    key: &FecSetKey,
    num_expected_data_shreds: u16,
) -> bool {
    if num_expected_data_shreds == 0 {
        return false;
    }
    let start = key.fec_set_index as usize;
    let Some(end_excl) = start.checked_add(num_expected_data_shreds as usize) else {
        return false;
    };
    if end_excl > tracker.data_shreds.len() {
        return false;
    }

    (start..end_excl).all(|idx| {
        tracker.data_shreds[idx]
            .as_ref()
            .is_some_and(|s| shred_matches_fec_key(s, key))
            || tracker.already_deshredded[idx]
    })
}
#[cfg(test)]
mod tests {
    use std::{
        collections::{hash_map::Entry, HashSet},
        io::{Cursor, Read, Write},
        net::UdpSocket,
        sync::{atomic::Ordering, Arc},
        time::Instant,
    };

    use bincode::Options;
    use borsh::BorshDeserialize;
    use itertools::Itertools;
    use rand::Rng;
    use solana_ledger::{
        blockstore::make_slot_entries_with_transactions,
        shred::{merkle::Shred, ProcessShredsStats, ReedSolomonCache, ShredCommonHeader, Shredder},
    };
    use solana_perf::packet::{Packet, PacketBatch};
    use solana_sdk::{clock::Slot, hash::Hash, signature::Keypair};

    use super::BINCODE_DESERIALIZE_LIMIT_BYTES;
    use crate::forwarder::ShredMetrics;

    // Fixture expectations: these are regression baselines. If decoding improves, bump them.
    const FIXTURE_SERIALIZED_SHREDS_TOTAL_ENTRIES: usize = 13_580;
    const FIXTURE_SERIALIZED_SHREDS_DECODED_SLOTS: usize = 29;
    const FIXTURE_SERIALIZED_SHREDS_DECODED_SETS: usize = 609;
    const FIXTURE_SERIALIZED_SHREDS_DECODED_DATA_SHREDS: usize = 22_017;

    const FIXTURE_DATA_COMPLETE_TOTAL_ENTRIES: usize = 43_170;
    const FIXTURE_DATA_COMPLETE_DECODED_SLOTS: usize = 61;
    const FIXTURE_DATA_COMPLETE_DECODED_SETS: usize = 1_419;
    const FIXTURE_DATA_COMPLETE_DECODED_DATA_SHREDS: usize = 54_284;

    const TEST_RECONSTRUCT_CFG_DECODE_ENTRIES: super::ReconstructShredsConfig =
        super::ReconstructShredsConfig {
            slot_lookback: 50,
            slot_future: 50,
            unknown_start_max_positions: 1,
            known_start_parity_scrub_enabled: false,
            known_start_parity_scrub_max_fec_sets_per_failure: 1,
            known_start_parity_scrub_max_indices_per_fec: 3,
            known_start_parity_scrub_max_attempts_per_fec_generation: 1,
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

    fn packet_from_payload(payload: &[u8]) -> Packet {
        let mut packet = Packet::default();
        packet.buffer_mut()[..payload.len()].copy_from_slice(payload);
        packet.meta_mut().size = payload.len();
        packet
    }

    fn assert_decoded_entries_sane(
        deshredded_entries: &[(Slot, Vec<solana_entry::entry::Entry>, Vec<u8>)],
    ) {
        // Keep these checks cheap: the fixture tests already do a lot of work.
        const MAX_HASH_CHAIN_VECS: usize = 64;

        let mut seen_by_slot =
            ahash::HashMap::<Slot, ahash::AHashSet<solana_sdk::signature::Signature>>::default();
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
                        .first()
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

    fn deserialize_entries_allow_trailing(
        deshredded_payload: &[u8],
    ) -> Vec<solana_entry::entry::Entry> {
        let bincode_opts = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .with_limit(BINCODE_DESERIALIZE_LIMIT_BYTES as u64);
        let mut cursor = Cursor::new(deshredded_payload);
        bincode_opts
            .allow_trailing_bytes()
            .deserialize_from::<_, Vec<solana_entry::entry::Entry>>(&mut cursor)
            .unwrap()
    }

    fn build_tracker_with_mismatched_fec_data(
        slot: Slot,
    ) -> (super::ShredsStateTracker, super::FecSetKey, u16, usize) {
        let leader_keypair = Arc::new(Keypair::new());
        let reed_solomon_cache = ReedSolomonCache::default();
        let shredder = Shredder::new(slot, slot - 1, 0, 0).unwrap();
        let entries = make_slot_entries_with_transactions(64);
        let (data_shreds, _coding_shreds) = shredder.entries_to_shreds(
            &leader_keypair,
            entries.as_slice(),
            true,
            Some(Hash::new_from_array([5u8; 32])),
            0,
            0,
            true,
            &reed_solomon_cache,
            &mut ProcessShredsStats::default(),
        );
        assert_eq!(data_shreds.len(), 32);

        let canonical_data_shreds = data_shreds
            .iter()
            .sorted_by_key(|s| s.index())
            .map(|s| Shred::from_payload(s.payload().clone()).unwrap())
            .collect_vec();
        let key = super::FecSetKey::from_shred(canonical_data_shreds.first().unwrap());
        let num_data_shreds = canonical_data_shreds
            .len()
            .try_into()
            .expect("data_shreds length should fit in u16");

        let mismatched_idx = canonical_data_shreds.len() / 4;
        assert!(mismatched_idx > 0);
        let mut mismatched_payload = canonical_data_shreds[mismatched_idx].payload().to_vec();
        let canonical_fec_set_index =
            u32::from_le_bytes(mismatched_payload[79..83].try_into().unwrap());
        let mismatched_fec_set_index = canonical_fec_set_index.saturating_add(1);
        assert!(
            mismatched_fec_set_index <= canonical_data_shreds[mismatched_idx].index(),
            "mismatched fec_set_index must remain <= index so sanitize passes"
        );
        mismatched_payload[79..83].copy_from_slice(&mismatched_fec_set_index.to_le_bytes());
        let mismatched_shred = Shred::from_payload(mismatched_payload).unwrap();
        assert_ne!(
            mismatched_shred.fec_set_index(),
            canonical_data_shreds[mismatched_idx].fec_set_index()
        );

        let mut tracker = super::ShredsStateTracker::default();
        for shred in canonical_data_shreds {
            let idx = shred.index() as usize;
            tracker.data_shreds[idx] = Some(shred);
        }
        tracker.data_shreds[mismatched_idx] = Some(mismatched_shred);

        (tracker, key, num_data_shreds, mismatched_idx)
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

    // -----------------------------------------------------------------------------
    // Existing tests (fixture-based and synthetic end-to-end).
    // -----------------------------------------------------------------------------

    #[derive(Clone, Copy)]
    struct LiveFixtureCase {
        name: &'static str,
        filepath: &'static str,
        expected_packet_count: usize,
        expected_decoded_shred_count: usize,
        expected_unique_header_count: usize,
        min_total_entries: usize,
        min_decoded_slots: usize,
        min_decoded_sets: usize,
        min_decoded_data_shreds: usize,
        expected_slots_tracked_full: usize,
        expected_slots_tracked_drop33: usize,
    }

    #[derive(Clone, Copy)]
    struct LiveFixtureScenario {
        name: &'static str,
        drop_every_third_packet: bool,
        require_recovery: bool,
    }

    struct LiveFixtureRunSummary {
        recovered_count: usize,
        total_entries: usize,
        decoded_slots: usize,
        decoded_sets: usize,
        decoded_data_shreds: usize,
        slots_tracked: usize,
    }

    fn load_fixture_packets(filepath: &str) -> Packets {
        let mut file = std::fs::File::open(filepath).unwrap();
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).unwrap();
        Packets::try_from_slice(&buffer).unwrap()
    }

    fn fixture_packets_to_batch(
        raw_packets: &[Vec<u8>],
        drop_every_third_packet: bool,
    ) -> PacketBatch {
        PacketBatch::new(
            raw_packets
                .iter()
                .enumerate()
                .filter(|(index, _)| !drop_every_third_packet || (index + 1) % 3 != 0)
                .map(|(_, payload)| {
                    let mut packet = Packet::default();
                    packet.buffer_mut()[..payload.len()].copy_from_slice(payload);
                    packet.meta_mut().size = payload.len();
                    packet
                })
                .collect_vec(),
        )
    }

    fn run_live_fixture_scenario(
        raw_packets: &[Vec<u8>],
        scenario: LiveFixtureScenario,
    ) -> LiveFixtureRunSummary {
        let rs_cache = ReedSolomonCache::default();
        let metrics = Arc::new(ShredMetrics::default());
        let mut all_shreds = ahash::HashMap::default();
        let mut slot_fec_keys_to_iterate: Vec<(Slot, super::FecSetKey)> = Vec::new();
        let mut deshredded_entries = Vec::new();
        let mut highest_slot_seen = 0;
        let packet_batch = fixture_packets_to_batch(raw_packets, scenario.drop_every_third_packet);
        let mut scratch = super::ReconstructScratch::default();
        let recovered_count = super::reconstruct_shreds(
            vec![packet_batch],
            &mut all_shreds,
            &mut slot_fec_keys_to_iterate,
            &mut deshredded_entries,
            &mut highest_slot_seen,
            &rs_cache,
            TEST_RECONSTRUCT_CFG_DECODE_ENTRIES,
            &metrics,
            &mut scratch,
        );

        let decoded_data_shreds = all_shreds
            .values()
            .map(|(_fec_sets, tracker)| tracker.already_deshredded.iter_ones().count())
            .sum::<usize>();
        assert_decoded_entries_sane(&deshredded_entries);

        let total_entries = deshredded_entries
            .iter()
            .map(|(_slot, entries, _entries_bytes)| entries.len())
            .sum::<usize>();
        let decoded_slots = deshredded_entries
            .iter()
            .map(|(slot, _entries, _entries_bytes)| *slot)
            .collect::<ahash::AHashSet<_>>()
            .len();
        let decoded_sets = deshredded_entries.len();
        let slots_tracked = all_shreds.len();

        LiveFixtureRunSummary {
            recovered_count,
            total_entries,
            decoded_slots,
            decoded_sets,
            decoded_data_shreds,
            slots_tracked,
        }
    }

    #[test]
    fn test_reconstruct_live_fixture_matrix() {
        const FIXTURE_CASES: [LiveFixtureCase; 2] = [
            LiveFixtureCase {
                name: "serialized_shreds",
                filepath: "../bins/serialized_shreds.bin",
                expected_packet_count: 50_000,
                expected_decoded_shred_count: 49_989,
                expected_unique_header_count: 44_900,
                min_total_entries: FIXTURE_SERIALIZED_SHREDS_TOTAL_ENTRIES,
                min_decoded_slots: FIXTURE_SERIALIZED_SHREDS_DECODED_SLOTS,
                min_decoded_sets: FIXTURE_SERIALIZED_SHREDS_DECODED_SETS,
                min_decoded_data_shreds: FIXTURE_SERIALIZED_SHREDS_DECODED_DATA_SHREDS,
                expected_slots_tracked_full: 30,
                expected_slots_tracked_drop33: 29,
            },
            LiveFixtureCase {
                name: "data_complete",
                filepath: "../bins/serialized_shreds_data_complete_test.bin",
                expected_packet_count: 150_000,
                expected_decoded_shred_count: 149_977,
                expected_unique_header_count: 109_221,
                min_total_entries: FIXTURE_DATA_COMPLETE_TOTAL_ENTRIES,
                min_decoded_slots: FIXTURE_DATA_COMPLETE_DECODED_SLOTS,
                min_decoded_sets: FIXTURE_DATA_COMPLETE_DECODED_SETS,
                min_decoded_data_shreds: FIXTURE_DATA_COMPLETE_DECODED_DATA_SHREDS,
                expected_slots_tracked_full: 61,
                expected_slots_tracked_drop33: 61,
            },
        ];
        const SCENARIOS: [LiveFixtureScenario; 2] = [
            LiveFixtureScenario {
                name: "full",
                drop_every_third_packet: false,
                require_recovery: false,
            },
            LiveFixtureScenario {
                name: "drop33",
                drop_every_third_packet: true,
                require_recovery: true,
            },
        ];

        for fixture in FIXTURE_CASES {
            let packets = load_fixture_packets(fixture.filepath);
            assert_eq!(
                packets.packets.len(),
                fixture.expected_packet_count,
                "fixture {} packet count mismatch",
                fixture.name
            );

            let shreds = packets
                .packets
                .iter()
                .filter_map(|p| Shred::from_payload(p.clone()).ok())
                .collect::<Vec<_>>();
            assert_eq!(
                shreds.len(),
                fixture.expected_decoded_shred_count,
                "fixture {} decoded shred count mismatch",
                fixture.name
            );

            let unique_headers = packets
                .packets
                .iter()
                .filter_map(|p| {
                    Shred::from_payload(p.clone())
                        .ok()
                        .map(|s| *s.common_header())
                })
                .collect::<HashSet<ShredCommonHeader>>();
            assert_eq!(
                unique_headers.len(),
                fixture.expected_unique_header_count,
                "fixture {} unique header count mismatch",
                fixture.name
            );

            for scenario in SCENARIOS {
                let summary = run_live_fixture_scenario(&packets.packets, scenario);
                let scenario_name = format!("{}:{}", fixture.name, scenario.name);

                if scenario.require_recovery {
                    assert!(
                        summary.recovered_count > 0,
                        "{scenario_name} expected recovery"
                    );
                }
                assert!(
                    summary.total_entries >= fixture.min_total_entries,
                    "{scenario_name} total_entries: {}",
                    summary.total_entries
                );
                assert!(
                    summary.decoded_slots >= fixture.min_decoded_slots,
                    "{scenario_name} decoded_slots: {}",
                    summary.decoded_slots
                );
                assert!(
                    summary.decoded_sets >= fixture.min_decoded_sets,
                    "{scenario_name} decoded_sets: {}",
                    summary.decoded_sets
                );
                assert!(
                    summary.decoded_data_shreds >= fixture.min_decoded_data_shreds,
                    "{scenario_name} decoded_data_shreds: {}",
                    summary.decoded_data_shreds
                );

                let expected_slots_tracked = if scenario.drop_every_third_packet {
                    fixture.expected_slots_tracked_drop33
                } else {
                    fixture.expected_slots_tracked_full
                };
                assert_eq!(
                    summary.slots_tracked, expected_slots_tracked,
                    "{scenario_name} slots_tracked mismatch"
                );
            }
        }
    }

    #[test]
    #[ignore = "manual report helper for fixture decode coverage"]
    fn report_live_fixture_matrix_summary() {
        const FIXTURE_CASES: [LiveFixtureCase; 2] = [
            LiveFixtureCase {
                name: "serialized_shreds",
                filepath: "../bins/serialized_shreds.bin",
                expected_packet_count: 50_000,
                expected_decoded_shred_count: 49_989,
                expected_unique_header_count: 44_900,
                min_total_entries: FIXTURE_SERIALIZED_SHREDS_TOTAL_ENTRIES,
                min_decoded_slots: FIXTURE_SERIALIZED_SHREDS_DECODED_SLOTS,
                min_decoded_sets: FIXTURE_SERIALIZED_SHREDS_DECODED_SETS,
                min_decoded_data_shreds: FIXTURE_SERIALIZED_SHREDS_DECODED_DATA_SHREDS,
                expected_slots_tracked_full: 30,
                expected_slots_tracked_drop33: 29,
            },
            LiveFixtureCase {
                name: "data_complete",
                filepath: "../bins/serialized_shreds_data_complete_test.bin",
                expected_packet_count: 150_000,
                expected_decoded_shred_count: 149_977,
                expected_unique_header_count: 109_221,
                min_total_entries: FIXTURE_DATA_COMPLETE_TOTAL_ENTRIES,
                min_decoded_slots: FIXTURE_DATA_COMPLETE_DECODED_SLOTS,
                min_decoded_sets: FIXTURE_DATA_COMPLETE_DECODED_SETS,
                min_decoded_data_shreds: FIXTURE_DATA_COMPLETE_DECODED_DATA_SHREDS,
                expected_slots_tracked_full: 61,
                expected_slots_tracked_drop33: 61,
            },
        ];
        const SCENARIOS: [LiveFixtureScenario; 2] = [
            LiveFixtureScenario {
                name: "full",
                drop_every_third_packet: false,
                require_recovery: false,
            },
            LiveFixtureScenario {
                name: "drop33",
                drop_every_third_packet: true,
                require_recovery: true,
            },
        ];

        for fixture in FIXTURE_CASES {
            let packets = load_fixture_packets(fixture.filepath);
            eprintln!(
                "[fixture:{}] packets={} decoded_shreds_expected={} unique_headers_expected={}",
                fixture.name,
                packets.packets.len(),
                fixture.expected_decoded_shred_count,
                fixture.expected_unique_header_count
            );

            for scenario in SCENARIOS {
                let summary = run_live_fixture_scenario(&packets.packets, scenario);
                eprintln!(
                    "  - scenario={} recovered={} entries={} slots={} sets={} data_shreds={} slots_tracked={} deltas(entries:+{},slots:+{},sets:+{},data_shreds:+{})",
                    scenario.name,
                    summary.recovered_count,
                    summary.total_entries,
                    summary.decoded_slots,
                    summary.decoded_sets,
                    summary.decoded_data_shreds,
                    summary.slots_tracked,
                    summary.total_entries.saturating_sub(fixture.min_total_entries),
                    summary.decoded_slots.saturating_sub(fixture.min_decoded_slots),
                    summary.decoded_sets.saturating_sub(fixture.min_decoded_sets),
                    summary
                        .decoded_data_shreds
                        .saturating_sub(fixture.min_decoded_data_shreds),
                );
            }
        }
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

        // Model a real leader producing multiple completed data sets within one slot: only the
        // final entry group should set `LAST_SHRED_IN_SLOT`.
        let mut next_shred_index: u32 = 0;
        let mut next_code_index: u32 = 0;
        (0..num_entry_groups).for_each(|i| {
            let is_last_in_slot = i + 1 == num_entry_groups;
            let _entries = make_slot_entries_with_transactions(num_entries);
            let (_data_shreds, _coding_shreds) = shredder.entries_to_shreds(
                &leader_keypair,
                _entries.as_slice(),
                is_last_in_slot,
                chained_merkle_root,
                next_shred_index,
                next_code_index,
                true, // merkle_variant
                &reed_solomon_cache,
                &mut ProcessShredsStats::default(),
            );
            next_shred_index += _data_shreds.len() as u32;
            next_code_index += _coding_shreds.len() as u32;
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
        assert_eq!(
            data_shreds.iter().filter(|s| s.last_in_slot()).count(),
            1,
            "expected exactly one LAST_SHRED_IN_SLOT in the slot"
        );
        assert_eq!(
            data_shreds
                .iter()
                .filter(|s| s.data_complete() || s.last_in_slot())
                .count(),
            num_entry_groups,
            "expected exactly one DATA_COMPLETE_SHRED boundary per entry group"
        );
        assert_eq!(
            data_shreds
                .iter()
                .map(|s| s.fec_set_index())
                .dedup()
                .count(),
            num_entry_groups,
            "expected one FEC set per entry group in this test setup"
        );

        let metrics = Arc::new(ShredMetrics::default());
        let rs_cache = ReedSolomonCache::default();

        // Test 1: all shreds provided
        let mut all_shreds = ahash::HashMap::default();
        let mut slot_fec_keys_to_iterate: Vec<(Slot, super::FecSetKey)> = Vec::new();
        let mut deshredded_entries = Vec::new();
        let mut highest_slot_seen = 0;
        let packet_batch = PacketBatch::new(packets.clone());
        let mut scratch = super::ReconstructScratch::default();
        let recovered_count = super::reconstruct_shreds(
            vec![packet_batch],
            &mut all_shreds,
            &mut slot_fec_keys_to_iterate,
            &mut deshredded_entries,
            &mut highest_slot_seen,
            &rs_cache,
            TEST_RECONSTRUCT_CFG_DECODE_ENTRIES,
            &metrics,
            &mut scratch,
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
        let mut slot_fec_keys_to_iterate: Vec<(Slot, super::FecSetKey)> = Vec::new();
        let mut deshredded_entries = Vec::new();
        let mut highest_slot_seen = 0;
        let packet_batch = PacketBatch::new(
            packets
                .iter()
                .enumerate()
                .filter(|(index, _)| (index + 1) % 3 != 0)
                .map(|(_i, p)| p.clone())
                .collect(),
        );
        let mut scratch = super::ReconstructScratch::default();
        let recovered_count = super::reconstruct_shreds(
            vec![packet_batch],
            &mut all_shreds,
            &mut slot_fec_keys_to_iterate,
            &mut deshredded_entries,
            &mut highest_slot_seen,
            &rs_cache,
            TEST_RECONSTRUCT_CFG_DECODE_ENTRIES,
            &metrics,
            &mut scratch,
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

    #[test]
    fn test_counts_entries_and_txns() {
        let slot = 777_777;
        let leader_keypair = Arc::new(Keypair::new());
        let reed_solomon_cache = ReedSolomonCache::default();
        let shredder = Shredder::new(slot, slot - 1, 0, 0).unwrap();

        let entries = make_slot_entries_with_transactions(16);
        let (data_shreds, coding_shreds) = shredder.entries_to_shreds(
            &leader_keypair,
            entries.as_slice(),
            true,
            Some(Hash::new_from_array([7u8; 32])),
            0,
            0,
            true,
            &reed_solomon_cache,
            &mut ProcessShredsStats::default(),
        );
        assert!(!data_shreds.is_empty());

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
        let mut slot_fec_keys_to_iterate: Vec<(Slot, super::FecSetKey)> = Vec::new();
        let mut deshredded_entries = Vec::new();
        let mut highest_slot_seen = 0;
        let packet_batch = PacketBatch::new(packets);
        let mut scratch = super::ReconstructScratch::default();
        super::reconstruct_shreds(
            vec![packet_batch],
            &mut all_shreds,
            &mut slot_fec_keys_to_iterate,
            &mut deshredded_entries,
            &mut highest_slot_seen,
            &rs_cache,
            TEST_RECONSTRUCT_CFG_DECODE_ENTRIES,
            &metrics,
            &mut scratch,
        );

        assert_eq!(deshredded_entries.len(), 1);
        assert_eq!(deshredded_entries[0].1.len(), entries.len());
        assert_eq!(
            bincode::deserialize::<Vec<solana_entry::entry::Entry>>(&deshredded_entries[0].2)
                .unwrap()
                .len(),
            entries.len()
        );
        assert_eq!(
            metrics.entry_count.load(Ordering::Relaxed),
            entries.len() as u64
        );
        let expected_txn_count: u64 = entries.iter().map(|e| e.transactions.len() as u64).sum();
        assert_eq!(
            metrics.txn_count.load(Ordering::Relaxed),
            expected_txn_count
        );
        assert_eq!(
            metrics
                .fec_set_decode_known_start_latency_count
                .load(Ordering::Relaxed),
            1,
            "expected one known-start completion latency sample"
        );
        assert_eq!(
            metrics
                .fec_set_decode_unknown_start_latency_count
                .load(Ordering::Relaxed),
            0,
            "expected no unknown-start completion latency samples"
        );
    }

    #[test]
    fn test_reconstruct_filters_version_mismatch_ingress() {
        let slot = 22_222;
        let leader_keypair = Arc::new(Keypair::new());
        let reed_solomon_cache = ReedSolomonCache::default();
        let shredder = Shredder::new(slot, slot - 1, 0, 0).unwrap();
        let entries = make_slot_entries_with_transactions(128);
        let (data_shreds, _coding_shreds) = shredder.entries_to_shreds(
            &leader_keypair,
            entries.as_slice(),
            true,
            None,
            0, // next_shred_index
            0, // next_code_index
            true,
            &reed_solomon_cache,
            &mut ProcessShredsStats::default(),
        );
        assert!(data_shreds.len() >= 3);

        let mut original_packet = Packet::default();
        data_shreds[0].copy_to_packet(&mut original_packet);
        let original_size = original_packet.meta().size;
        let original_bytes = &original_packet.buffer_mut()[..original_size];
        let original_version = u16::from_le_bytes([original_bytes[77], original_bytes[78]]);
        let poisoned_version = original_version.wrapping_add(1);
        let poisoned_index = data_shreds[0].index() as usize;

        let mut poisoned_packet = Packet::default();
        data_shreds[0].copy_to_packet(&mut poisoned_packet);
        let poisoned_size = poisoned_packet.meta().size;
        let poisoned_bytes = &mut poisoned_packet.buffer_mut()[..poisoned_size];
        poisoned_bytes[77..79].copy_from_slice(&poisoned_version.to_le_bytes());

        let mut packets = vec![poisoned_packet];
        for shred in data_shreds.iter().take(3) {
            let mut packet = Packet::default();
            shred.copy_to_packet(&mut packet);
            packets.push(packet);
        }

        let metrics = Arc::new(ShredMetrics::default());
        let mut all_shreds = ahash::HashMap::default();
        let mut slot_fec_keys_to_iterate: Vec<(Slot, super::FecSetKey)> = Vec::new();
        let mut deshredded_entries = Vec::new();
        let mut highest_slot_seen = 0;

        let packet_batch = PacketBatch::new(packets);
        let mut scratch = super::ReconstructScratch::default();
        super::reconstruct_shreds(
            vec![packet_batch],
            &mut all_shreds,
            &mut slot_fec_keys_to_iterate,
            &mut deshredded_entries,
            &mut highest_slot_seen,
            &reed_solomon_cache,
            TEST_RECONSTRUCT_CFG_DECODE_ENTRIES,
            &metrics,
            &mut scratch,
        );

        let (_fec_sets, state_tracker) = all_shreds.get(&slot).expect("slot must be present");
        let stored = state_tracker.data_shreds[poisoned_index]
            .as_ref()
            .expect("index should be ingested");
        assert_eq!(stored.common_header().version, original_version);
        assert_eq!(
            metrics
                .reconstruct_ingress_filter_drop_count
                .load(Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn test_reconstruct_dominant_version_vote_ignores_truncated_packets() {
        let slot = 22_222_001;
        let leader_keypair = Arc::new(Keypair::new());
        let reed_solomon_cache = ReedSolomonCache::default();
        let shredder = Shredder::new(slot, slot - 1, 0, 0).unwrap();
        let entries = make_slot_entries_with_transactions(128);
        let (data_shreds, _coding_shreds) = shredder.entries_to_shreds(
            &leader_keypair,
            entries.as_slice(),
            true,
            None,
            0, // next_shred_index
            0, // next_code_index
            true,
            &reed_solomon_cache,
            &mut ProcessShredsStats::default(),
        );
        assert!(data_shreds.len() >= 3);

        let original_payload = data_shreds[0].payload().to_vec();
        let original_version = u16::from_le_bytes([original_payload[77], original_payload[78]]);
        let poisoned_version = original_version.wrapping_add(1);

        // Build a truncated packet that still includes the version bytes. This packet must not
        // influence the dominant-version vote.
        let mut poisoned_payload = original_payload.clone();
        poisoned_payload[77..79].copy_from_slice(&poisoned_version.to_le_bytes());
        assert!(poisoned_payload.len() > 79);
        let truncated_len = poisoned_payload.len() - 1;
        let truncated_bytes = &poisoned_payload[..truncated_len];
        const TRUNCATED_COUNT: usize = 8;

        let mut packets = Vec::with_capacity(TRUNCATED_COUNT + 3);
        for _ in 0..TRUNCATED_COUNT {
            let mut packet = Packet::default();
            packet.buffer_mut()[..truncated_len].copy_from_slice(truncated_bytes);
            packet.meta_mut().size = truncated_len;
            packets.push(packet);
        }
        packets.extend(
            data_shreds
                .iter()
                .take(3)
                .map(|shred| packet_from_payload(shred.payload())),
        );

        let first_valid_index = data_shreds[0].index() as usize;
        let metrics = Arc::new(ShredMetrics::default());
        let mut all_shreds = ahash::HashMap::default();
        let mut slot_fec_keys_to_iterate: Vec<(Slot, super::FecSetKey)> = Vec::new();
        let mut deshredded_entries = Vec::new();
        let mut highest_slot_seen = 0;
        let packet_batch = PacketBatch::new(packets);
        let mut scratch = super::ReconstructScratch::default();
        super::reconstruct_shreds(
            vec![packet_batch],
            &mut all_shreds,
            &mut slot_fec_keys_to_iterate,
            &mut deshredded_entries,
            &mut highest_slot_seen,
            &reed_solomon_cache,
            TEST_RECONSTRUCT_CFG_DECODE_ENTRIES,
            &metrics,
            &mut scratch,
        );

        let (_fec_sets, state_tracker) = all_shreds.get(&slot).expect("slot must be present");
        let stored = state_tracker.data_shreds[first_valid_index]
            .as_ref()
            .expect("valid shred should survive dominant-version filtering");
        assert_eq!(stored.common_header().version, original_version);
        assert!(
            metrics
                .reconstruct_ingress_filter_drop_count
                .load(Ordering::Relaxed)
                >= TRUNCATED_COUNT as u64
        );
    }

    #[test]
    fn test_reconstruct_filters_invalid_last_flag_without_data_complete_ingress() {
        let slot = 22_223;
        let leader_keypair = Arc::new(Keypair::new());
        let reed_solomon_cache = ReedSolomonCache::default();
        let shredder = Shredder::new(slot, slot - 1, 0, 0).unwrap();
        let entries = make_slot_entries_with_transactions(64);
        let (data_shreds, _coding_shreds) = shredder.entries_to_shreds(
            &leader_keypair,
            entries.as_slice(),
            true,
            None,
            0, // next_shred_index
            0, // next_code_index
            true,
            &reed_solomon_cache,
            &mut ProcessShredsStats::default(),
        );
        assert!(data_shreds.len() >= 2);

        let first_index = data_shreds[0].index() as usize;
        let last_data_shred = data_shreds
            .iter()
            .max_by_key(|s| s.index())
            .expect("must have a terminal data shred");
        let poisoned_index = last_data_shred.index() as usize;

        let mut poisoned_payload = last_data_shred.payload().to_vec();
        // Invalid per spec: LAST_SHRED_IN_SLOT (bit 7) set while DATA_COMPLETE_SHRED (bit 6) unset.
        poisoned_payload[85] = (poisoned_payload[85] & 0x3F) | 0x80;
        assert_eq!(poisoned_payload[85] & 0xC0, 0x80);

        let packets = vec![
            packet_from_payload(data_shreds[0].payload()),
            packet_from_payload(&poisoned_payload),
        ];

        let metrics = Arc::new(ShredMetrics::default());
        let mut all_shreds = ahash::HashMap::default();
        let mut slot_fec_keys_to_iterate: Vec<(Slot, super::FecSetKey)> = Vec::new();
        let mut deshredded_entries = Vec::new();
        let mut highest_slot_seen = 0;

        let packet_batch = PacketBatch::new(packets);
        let mut scratch = super::ReconstructScratch::default();
        super::reconstruct_shreds(
            vec![packet_batch],
            &mut all_shreds,
            &mut slot_fec_keys_to_iterate,
            &mut deshredded_entries,
            &mut highest_slot_seen,
            &reed_solomon_cache,
            TEST_RECONSTRUCT_CFG_DECODE_ENTRIES,
            &metrics,
            &mut scratch,
        );

        let (_fec_sets, state_tracker) = all_shreds.get(&slot).expect("slot must be present");
        assert!(state_tracker.data_shreds[first_index].is_some());
        assert!(
            state_tracker.data_shreds[poisoned_index].is_none(),
            "invalid LAST-without-DATA_COMPLETE shred should be filtered at ingress"
        );
        assert!(
            deshredded_entries.is_empty(),
            "invalid terminal flags should not produce decoded entries"
        );
    }

    #[test]
    fn test_reconstruct_filters_invalid_data_size_ingress() {
        let slot = 22_224;
        let leader_keypair = Arc::new(Keypair::new());
        let reed_solomon_cache = ReedSolomonCache::default();
        let shredder = Shredder::new(slot, slot - 1, 0, 0).unwrap();
        let entries = make_slot_entries_with_transactions(64);
        let (data_shreds, _coding_shreds) = shredder.entries_to_shreds(
            &leader_keypair,
            entries.as_slice(),
            true,
            None,
            0, // next_shred_index
            0, // next_code_index
            true,
            &reed_solomon_cache,
            &mut ProcessShredsStats::default(),
        );
        assert!(data_shreds.len() >= 3);

        let invalid_low_index = data_shreds[0].index() as usize;
        let invalid_high_index = data_shreds[1].index() as usize;
        let valid_index = data_shreds[2].index() as usize;

        let mut invalid_low_payload = data_shreds[0].payload().to_vec();
        invalid_low_payload[86..88].copy_from_slice(&0u16.to_le_bytes());
        let mut invalid_high_payload = data_shreds[1].payload().to_vec();
        invalid_high_payload[86..88].copy_from_slice(&u16::MAX.to_le_bytes());

        let packets = vec![
            packet_from_payload(&invalid_low_payload),
            packet_from_payload(&invalid_high_payload),
            packet_from_payload(data_shreds[2].payload()),
        ];

        let metrics = Arc::new(ShredMetrics::default());
        let mut all_shreds = ahash::HashMap::default();
        let mut slot_fec_keys_to_iterate: Vec<(Slot, super::FecSetKey)> = Vec::new();
        let mut deshredded_entries = Vec::new();
        let mut highest_slot_seen = 0;

        let packet_batch = PacketBatch::new(packets);
        let mut scratch = super::ReconstructScratch::default();
        super::reconstruct_shreds(
            vec![packet_batch],
            &mut all_shreds,
            &mut slot_fec_keys_to_iterate,
            &mut deshredded_entries,
            &mut highest_slot_seen,
            &reed_solomon_cache,
            TEST_RECONSTRUCT_CFG_DECODE_ENTRIES,
            &metrics,
            &mut scratch,
        );

        let (_fec_sets, state_tracker) = all_shreds.get(&slot).expect("slot must be present");
        assert!(state_tracker.data_shreds[valid_index].is_some());
        assert!(
            state_tracker.data_shreds[invalid_low_index].is_none(),
            "invalid low data size shred should be filtered at ingress"
        );
        assert!(
            state_tracker.data_shreds[invalid_high_index].is_none(),
            "invalid high data size shred should be filtered at ingress"
        );
        assert!(
            deshredded_entries.is_empty(),
            "partial range with invalid shreds should not decode entries"
        );
    }

    #[test]
    fn test_reconstruct_filters_truncated_payload_shorter_than_variant_size() {
        let slot = 22_224_100;
        let leader_keypair = Arc::new(Keypair::new());
        let reed_solomon_cache = ReedSolomonCache::default();
        let shredder = Shredder::new(slot, slot - 1, 0, 0).unwrap();
        let entries = make_slot_entries_with_transactions(64);
        let (data_shreds, _coding_shreds) = shredder.entries_to_shreds(
            &leader_keypair,
            entries.as_slice(),
            true,
            None,
            0, // next_shred_index
            0, // next_code_index
            true,
            &reed_solomon_cache,
            &mut ProcessShredsStats::default(),
        );
        assert!(data_shreds.len() >= 2);

        let truncated_index = data_shreds[0].index() as usize;
        let valid_index = data_shreds[1].index() as usize;
        let truncated_payload = data_shreds[0].payload();
        assert!(
            truncated_payload.len() > 1,
            "expected non-empty shred payload for truncation test"
        );
        let truncated_len = truncated_payload.len() - 1;

        let mut truncated_packet = Packet::default();
        truncated_packet.buffer_mut()[..truncated_len]
            .copy_from_slice(&truncated_payload[..truncated_len]);
        truncated_packet.meta_mut().size = truncated_len;

        let packets = vec![
            truncated_packet,
            packet_from_payload(data_shreds[1].payload()),
        ];

        let metrics = Arc::new(ShredMetrics::default());
        let mut all_shreds = ahash::HashMap::default();
        let mut slot_fec_keys_to_iterate: Vec<(Slot, super::FecSetKey)> = Vec::new();
        let mut deshredded_entries = Vec::new();
        let mut highest_slot_seen = 0;

        let packet_batch = PacketBatch::new(packets);
        let mut scratch = super::ReconstructScratch::default();
        super::reconstruct_shreds(
            vec![packet_batch],
            &mut all_shreds,
            &mut slot_fec_keys_to_iterate,
            &mut deshredded_entries,
            &mut highest_slot_seen,
            &reed_solomon_cache,
            TEST_RECONSTRUCT_CFG_DECODE_ENTRIES,
            &metrics,
            &mut scratch,
        );

        let (_fec_sets, state_tracker) = all_shreds.get(&slot).expect("slot must be present");
        assert!(state_tracker.data_shreds[valid_index].is_some());
        assert!(
            state_tracker.data_shreds[truncated_index].is_none(),
            "truncated payload should be rejected before ingestion"
        );
        assert!(
            deshredded_entries.is_empty(),
            "partial range with truncated payload should not decode entries"
        );
    }

    #[test]
    fn test_reconstruct_filters_invalid_variant_tag_ingress() {
        let slot = 22_225;
        let leader_keypair = Arc::new(Keypair::new());
        let reed_solomon_cache = ReedSolomonCache::default();
        let shredder = Shredder::new(slot, slot - 1, 0, 0).unwrap();
        let entries = make_slot_entries_with_transactions(64);
        let (data_shreds, _coding_shreds) = shredder.entries_to_shreds(
            &leader_keypair,
            entries.as_slice(),
            true,
            None,
            0, // next_shred_index
            0, // next_code_index
            true,
            &reed_solomon_cache,
            &mut ProcessShredsStats::default(),
        );
        assert!(data_shreds.len() >= 2);

        let invalid_variant_index = data_shreds[0].index() as usize;
        let valid_index = data_shreds[1].index() as usize;

        let mut invalid_variant_payload = data_shreds[0].payload().to_vec();
        invalid_variant_payload[64] = (invalid_variant_payload[64] & 0x0F) | 0x50;
        assert_eq!(invalid_variant_payload[64] & 0xF0, 0x50);

        let packets = vec![
            packet_from_payload(&invalid_variant_payload),
            packet_from_payload(data_shreds[1].payload()),
        ];

        let metrics = Arc::new(ShredMetrics::default());
        let mut all_shreds = ahash::HashMap::default();
        let mut slot_fec_keys_to_iterate: Vec<(Slot, super::FecSetKey)> = Vec::new();
        let mut deshredded_entries = Vec::new();
        let mut highest_slot_seen = 0;

        let packet_batch = PacketBatch::new(packets);
        let mut scratch = super::ReconstructScratch::default();
        super::reconstruct_shreds(
            vec![packet_batch],
            &mut all_shreds,
            &mut slot_fec_keys_to_iterate,
            &mut deshredded_entries,
            &mut highest_slot_seen,
            &reed_solomon_cache,
            TEST_RECONSTRUCT_CFG_DECODE_ENTRIES,
            &metrics,
            &mut scratch,
        );

        let (_fec_sets, state_tracker) = all_shreds.get(&slot).expect("slot must be present");
        assert!(state_tracker.data_shreds[valid_index].is_some());
        assert!(
            state_tracker.data_shreds[invalid_variant_index].is_none(),
            "invalid shred variant tag should be filtered at ingress"
        );
        assert!(
            deshredded_entries.is_empty(),
            "partial range with invalid variant should not decode entries"
        );
        assert_eq!(
            metrics
                .reconstruct_ingress_filter_drop_count
                .load(Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn test_reconstruct_filters_invalid_coding_position_ingress() {
        let slot = 22_225_100;
        let leader_keypair = Arc::new(Keypair::new());
        let reed_solomon_cache = ReedSolomonCache::default();
        let shredder = Shredder::new(slot, slot - 1, 0, 0).unwrap();
        let entries = make_slot_entries_with_transactions(64);
        let (data_shreds, coding_shreds) = shredder.entries_to_shreds(
            &leader_keypair,
            entries.as_slice(),
            true,
            None,
            0, // next_shred_index
            0, // next_code_index
            true,
            &reed_solomon_cache,
            &mut ProcessShredsStats::default(),
        );
        assert!(!data_shreds.is_empty());
        assert!(!coding_shreds.is_empty());

        let valid_data_index = data_shreds[0].index() as usize;

        let mut invalid_coding_payload = coding_shreds[0].payload().to_vec();
        let num_coding_shreds =
            u16::from_le_bytes(invalid_coding_payload[85..87].try_into().unwrap());
        // Invalid per spec: position MUST be in [0, num_coding_shreds).
        invalid_coding_payload[87..89].copy_from_slice(&num_coding_shreds.to_le_bytes());
        assert_eq!(
            u16::from_le_bytes(invalid_coding_payload[87..89].try_into().unwrap()),
            num_coding_shreds
        );

        let packets = vec![
            packet_from_payload(data_shreds[0].payload()),
            packet_from_payload(&invalid_coding_payload),
        ];

        let metrics = Arc::new(ShredMetrics::default());
        let mut all_shreds = ahash::HashMap::default();
        let mut slot_fec_keys_to_iterate: Vec<(Slot, super::FecSetKey)> = Vec::new();
        let mut deshredded_entries = Vec::new();
        let mut highest_slot_seen = 0;

        let packet_batch = PacketBatch::new(packets);
        let mut scratch = super::ReconstructScratch::default();
        super::reconstruct_shreds(
            vec![packet_batch],
            &mut all_shreds,
            &mut slot_fec_keys_to_iterate,
            &mut deshredded_entries,
            &mut highest_slot_seen,
            &reed_solomon_cache,
            TEST_RECONSTRUCT_CFG_DECODE_ENTRIES,
            &metrics,
            &mut scratch,
        );

        let (fec_sets, state_tracker) = all_shreds.get(&slot).expect("slot must be present");
        assert!(state_tracker.data_shreds[valid_data_index].is_some());
        assert!(
            fec_sets.values().all(|state| state.coding_count == 0),
            "invalid coding position shred should be filtered before FEC ingestion"
        );
    }

    #[test]
    fn test_recovery_overwrites_conflicting_data_shred_and_decodes() {
        let slot = 232_323;
        let leader_keypair = Arc::new(Keypair::new());
        let reed_solomon_cache = ReedSolomonCache::default();
        let shredder = Shredder::new(slot, slot - 1, 0, 0).unwrap();
        let entries = make_slot_entries_with_transactions(64);
        let (data_shreds, coding_shreds) = shredder.entries_to_shreds(
            &leader_keypair,
            entries.as_slice(),
            true,
            Some(Hash::new_from_array([1u8; 32])),
            0,
            0,
            true,
            &reed_solomon_cache,
            &mut ProcessShredsStats::default(),
        );
        assert_eq!(data_shreds.len(), 32);
        assert_eq!(coding_shreds.len(), 32);

        let poisoned_index = data_shreds[0].index() as usize;
        let original_payload = data_shreds[0].payload().to_vec();

        // Poison one data shred:
        // - signature byte differs (so it won't match the canonical FEC key),
        // - first Vec<Entry> length bytes are corrupted (so using this payload should fail decode).
        let mut poisoned_payload = original_payload.clone();
        poisoned_payload[0] ^= 0x01;
        poisoned_payload[88..96].fill(0xFF);

        let mut poisoned_packet = Packet::default();
        poisoned_packet.buffer_mut()[..poisoned_payload.len()].copy_from_slice(&poisoned_payload);
        poisoned_packet.meta_mut().size = poisoned_payload.len();

        // Feed the poisoned data shred first, omit the canonical copy of that index, and include
        // all other data + coding shreds so FEC recovery can reconstruct the canonical shard.
        let packets = std::iter::once(poisoned_packet)
            .chain(
                data_shreds
                    .iter()
                    .filter(|s| s.index() as usize != poisoned_index)
                    .map(|s| packet_from_payload(s.payload())),
            )
            .chain(
                coding_shreds
                    .iter()
                    .map(|s| packet_from_payload(s.payload())),
            )
            .collect_vec();

        let metrics = Arc::new(ShredMetrics::default());
        let rs_cache = ReedSolomonCache::default();
        let mut all_shreds = ahash::HashMap::default();
        let mut slot_fec_keys_to_iterate: Vec<(Slot, super::FecSetKey)> = Vec::new();
        let mut deshredded_entries = Vec::new();
        let mut highest_slot_seen = 0;
        let packet_batch = PacketBatch::new(packets);
        let mut scratch = super::ReconstructScratch::default();
        let recovered_count = super::reconstruct_shreds(
            vec![packet_batch],
            &mut all_shreds,
            &mut slot_fec_keys_to_iterate,
            &mut deshredded_entries,
            &mut highest_slot_seen,
            &rs_cache,
            TEST_RECONSTRUCT_CFG_DECODE_ENTRIES,
            &metrics,
            &mut scratch,
        );
        assert!(
            recovered_count >= 1,
            "expected to recover at least the poisoned/missing data shred"
        );
        assert_eq!(
            deshredded_entries
                .iter()
                .map(|(_slot, e, _bytes)| e.len())
                .sum::<usize>(),
            entries.len(),
            "expected successful decode after overwrite with recovered data shred"
        );
        let emitted_payload = &deshredded_entries[0].2;
        assert_ne!(
            &emitted_payload[..8],
            &[0xFF; 8],
            "expected emitted deshredded payload not to use poisoned vec-length prefix"
        );
        let decoded =
            bincode::deserialize::<Vec<solana_entry::entry::Entry>>(emitted_payload).unwrap();
        assert_eq!(decoded.len(), entries.len());
    }

    #[test]
    fn test_incoming_conflicting_data_duplicate_overwrites_and_decodes() {
        let slot = 242_424;
        let leader_keypair = Arc::new(Keypair::new());
        let reed_solomon_cache = ReedSolomonCache::default();
        let shredder = Shredder::new(slot, slot - 1, 0, 0).unwrap();
        let entries = make_slot_entries_with_transactions(64);
        let (data_shreds, _coding_shreds) = shredder.entries_to_shreds(
            &leader_keypair,
            entries.as_slice(),
            true,
            Some(Hash::new_from_array([6u8; 32])),
            0,
            0,
            true,
            &reed_solomon_cache,
            &mut ProcessShredsStats::default(),
        );
        assert!(data_shreds.len() >= 2);

        let poisoned_index = data_shreds[0].index() as usize;
        let mut poisoned_payload = data_shreds[0].payload().to_vec();
        poisoned_payload[88..96].fill(0xFF);

        // Feed poisoned duplicate first, then feed all canonical data shreds including that index.
        let packets = std::iter::once(packet_from_payload(&poisoned_payload))
            .chain(data_shreds.iter().map(|s| packet_from_payload(s.payload())))
            .collect_vec();

        let metrics = Arc::new(ShredMetrics::default());
        let rs_cache = ReedSolomonCache::default();
        let mut all_shreds = ahash::HashMap::default();
        let mut slot_fec_keys_to_iterate: Vec<(Slot, super::FecSetKey)> = Vec::new();
        let mut deshredded_entries = Vec::new();
        let mut highest_slot_seen = 0;
        let packet_batch = PacketBatch::new(packets);
        let mut scratch = super::ReconstructScratch::default();
        let recovered_count = super::reconstruct_shreds(
            vec![packet_batch],
            &mut all_shreds,
            &mut slot_fec_keys_to_iterate,
            &mut deshredded_entries,
            &mut highest_slot_seen,
            &rs_cache,
            TEST_RECONSTRUCT_CFG_DECODE_ENTRIES,
            &metrics,
            &mut scratch,
        );

        assert_eq!(recovered_count, 0);
        assert_eq!(
            deshredded_entries
                .iter()
                .map(|(_slot, e, _bytes)| e.len())
                .sum::<usize>(),
            entries.len(),
            "expected later canonical incoming duplicate to overwrite poisoned copy"
        );
        let stored_payload = all_shreds
            .get(&slot)
            .and_then(|(_, tracker)| tracker.data_shreds[poisoned_index].as_ref())
            .map(|s| s.payload().to_vec())
            .expect("poisoned index should be present in tracker");
        assert_eq!(
            &stored_payload[88..96],
            &data_shreds[0].payload()[88..96],
            "expected tracker to keep the canonical duplicate payload"
        );
    }

    #[test]
    fn test_incoming_conflicting_data_duplicate_smaller_size_overwrites_without_underflow() {
        let slot = 242_425;
        let leader_keypair = Arc::new(Keypair::new());
        let reed_solomon_cache = ReedSolomonCache::default();
        let shredder = Shredder::new(slot, slot - 1, 0, 0).unwrap();
        let entries = make_slot_entries_with_transactions(64);
        let (data_shreds, _coding_shreds) = shredder.entries_to_shreds(
            &leader_keypair,
            entries.as_slice(),
            true,
            Some(Hash::new_from_array([12u8; 32])),
            0,
            0,
            true,
            &reed_solomon_cache,
            &mut ProcessShredsStats::default(),
        );
        assert!(!data_shreds.is_empty());

        let canonical = data_shreds
            .iter()
            .find(|s| u16::from_le_bytes(s.payload()[86..88].try_into().unwrap()) > 88)
            .expect("expected at least one data shred with non-empty payload");
        let canonical_index = canonical.index() as usize;
        let mut smaller_payload = canonical.payload().to_vec();
        let canonical_size = u16::from_le_bytes(smaller_payload[86..88].try_into().unwrap());
        assert!(canonical_size > 88);
        let smaller_size = canonical_size - 1;
        smaller_payload[86..88].copy_from_slice(&smaller_size.to_le_bytes());

        // Feed canonical first, then a conflicting duplicate with a smaller (still valid) size.
        // This exercises conflict logging with negative size/data-length deltas.
        let packets = vec![
            packet_from_payload(canonical.payload()),
            packet_from_payload(&smaller_payload),
        ];

        let metrics = Arc::new(ShredMetrics::default());
        let rs_cache = ReedSolomonCache::default();
        let mut all_shreds = ahash::HashMap::default();
        let mut slot_fec_keys_to_iterate: Vec<(Slot, super::FecSetKey)> = Vec::new();
        let mut deshredded_entries = Vec::new();
        let mut highest_slot_seen = 0;
        let packet_batch = PacketBatch::new(packets);
        let mut scratch = super::ReconstructScratch::default();
        let recovered_count = super::reconstruct_shreds(
            vec![packet_batch],
            &mut all_shreds,
            &mut slot_fec_keys_to_iterate,
            &mut deshredded_entries,
            &mut highest_slot_seen,
            &rs_cache,
            TEST_RECONSTRUCT_CFG_DECODE_ENTRIES,
            &metrics,
            &mut scratch,
        );

        assert_eq!(recovered_count, 0);
        assert!(
            deshredded_entries.is_empty(),
            "single-index duplicate stream should not decode entries"
        );
        let stored_size = all_shreds
            .get(&slot)
            .and_then(|(_, tracker)| tracker.data_shreds[canonical_index].as_ref())
            .map(|s| u16::from_le_bytes(s.payload()[86..88].try_into().unwrap()))
            .expect("canonical index should be present in tracker");
        assert_eq!(
            stored_size, smaller_size,
            "conflicting duplicate with smaller size should overwrite existing payload"
        );
    }

    #[test]
    fn test_ingest_conflicting_duplicate_with_invalid_size_does_not_underflow() {
        let slot = 242_426;
        let leader_keypair = Arc::new(Keypair::new());
        let reed_solomon_cache = ReedSolomonCache::default();
        let shredder = Shredder::new(slot, slot - 1, 0, 0).unwrap();
        let entries = make_slot_entries_with_transactions(64);
        let (data_shreds, _coding_shreds) = shredder.entries_to_shreds(
            &leader_keypair,
            entries.as_slice(),
            true,
            Some(Hash::new_from_array([13u8; 32])),
            0,
            0,
            true,
            &reed_solomon_cache,
            &mut ProcessShredsStats::default(),
        );
        assert!(!data_shreds.is_empty());

        let canonical_payload = data_shreds[0].payload().to_vec();
        let canonical = Shred::from_payload(canonical_payload).unwrap();
        let mut invalid = canonical.clone();
        if let Shred::ShredData(ref mut data) = invalid {
            data.data_header.size = 0;
            data.payload[86..88].copy_from_slice(&0u16.to_le_bytes());
        } else {
            panic!("expected data shred");
        }
        let index = canonical.index() as usize;

        let mut tracker = super::ShredsStateTracker::default();
        assert!(super::ingest_data_shred(
            canonical.clone(),
            &mut tracker,
            super::DataShredSource::Incoming
        ));
        assert!(super::ingest_data_shred(
            invalid,
            &mut tracker,
            super::DataShredSource::Recovered
        ));
        assert_eq!(
            tracker.data_generation, 2,
            "both inserts should be accounted for without arithmetic underflow"
        );
        assert!(
            tracker.data_shreds[index].is_some(),
            "index should still contain a shred after overwrite"
        );
    }

    #[test]
    fn test_decode_entries_from_shred_range_missing_shred_returns_error() {
        let to_deshred: Vec<Option<Shred>> = vec![None];
        let err = super::decode_entries_from_shred_range(&to_deshred, false).unwrap_err();
        assert!(matches!(
            err,
            super::DecodeEntriesError::MissingShred { relative_index: 0 }
        ));
    }

    #[test]
    fn test_ingest_data_duplicate_retransmitter_signature_only_is_not_conflict() {
        let slot = 242_427;
        let leader_keypair = Arc::new(Keypair::new());
        let reed_solomon_cache = ReedSolomonCache::default();
        let shredder = Shredder::new(slot, slot - 1, 0, 0).unwrap();
        let entries = make_slot_entries_with_transactions(64);
        let (data_shreds, _coding_shreds) = shredder.entries_to_shreds(
            &leader_keypair,
            entries.as_slice(),
            true,
            Some(Hash::new_from_array([14u8; 32])),
            0,
            0,
            true,
            &reed_solomon_cache,
            &mut ProcessShredsStats::default(),
        );

        let target = data_shreds
            .iter()
            .find(|s| {
                matches!(
                    solana_ledger::shred::layout::get_shred_variant(s.payload()),
                    Ok(solana_ledger::shred::ShredVariant::MerkleData { resigned: true, .. })
                )
            })
            .expect("expected at least one resigned data shred");
        let mut duplicate_payload = target.payload().to_vec();
        let suffix_start = duplicate_payload
            .len()
            .saturating_sub(solana_ledger::shred::SIZE_OF_SIGNATURE);
        assert!(suffix_start < duplicate_payload.len());
        duplicate_payload[suffix_start..].fill(0xAB);
        assert_ne!(
            &duplicate_payload[suffix_start..],
            &target.payload()[suffix_start..]
        );

        let original = Shred::from_payload(target.payload().to_vec()).unwrap();
        let duplicate = Shred::from_payload(duplicate_payload).unwrap();
        let index = original.index() as usize;

        let mut tracker = super::ShredsStateTracker::default();
        assert!(super::ingest_data_shred(
            original.clone(),
            &mut tracker,
            super::DataShredSource::Incoming
        ));
        assert!(!super::ingest_data_shred(
            duplicate,
            &mut tracker,
            super::DataShredSource::Incoming
        ));
        assert_eq!(
            tracker.data_generation, 1,
            "retransmitter-signature-only duplicate should not overwrite data payload"
        );
        let stored = tracker.data_shreds[index]
            .as_ref()
            .map(|s| s.payload().to_vec())
            .expect("index should contain original payload");
        assert_eq!(stored.as_slice(), original.payload().as_ref());
    }

    #[test]
    fn test_insert_coding_duplicate_retransmitter_signature_only_is_not_conflict() {
        let slot = 242_428;
        let leader_keypair = Arc::new(Keypair::new());
        let reed_solomon_cache = ReedSolomonCache::default();
        let shredder = Shredder::new(slot, slot - 1, 0, 0).unwrap();
        let entries = make_slot_entries_with_transactions(64);
        let (_data_shreds, coding_shreds) = shredder.entries_to_shreds(
            &leader_keypair,
            entries.as_slice(),
            true,
            Some(Hash::new_from_array([15u8; 32])),
            0,
            0,
            true,
            &reed_solomon_cache,
            &mut ProcessShredsStats::default(),
        );

        let target = coding_shreds
            .iter()
            .find(|s| {
                matches!(
                    solana_ledger::shred::layout::get_shred_variant(s.payload()),
                    Ok(solana_ledger::shred::ShredVariant::MerkleCode { resigned: true, .. })
                )
            })
            .expect("expected at least one resigned coding shred");
        let mut duplicate_payload = target.payload().to_vec();
        let suffix_start = duplicate_payload
            .len()
            .saturating_sub(solana_ledger::shred::SIZE_OF_SIGNATURE);
        assert!(suffix_start < duplicate_payload.len());
        duplicate_payload[suffix_start..].fill(0xAB);
        assert_ne!(
            &duplicate_payload[suffix_start..],
            &target.payload()[suffix_start..]
        );

        let original = Shred::from_payload(target.payload().to_vec()).unwrap();
        let duplicate = Shred::from_payload(duplicate_payload).unwrap();
        let position = match &original {
            Shred::ShredCode(code) => code.coding_header.position as usize,
            Shred::ShredData(_) => panic!("expected coding shred"),
        };

        let mut fec_state = super::FecSetState::default();
        assert!(fec_state.insert_coding_shred(original.clone()));
        assert!(!fec_state.insert_coding_shred(duplicate));
        assert_eq!(
            fec_state.coding_count, 1,
            "retransmitter-signature-only duplicate should not replace coding shard"
        );
        assert_eq!(fec_state.generation, 1);
        let stored = fec_state.coding_by_pos[position]
            .as_ref()
            .map(|s| s.payload().to_vec())
            .expect("coding position should contain original payload");
        assert_eq!(stored.as_slice(), original.payload().as_ref());
    }

    #[test]
    fn test_late_canonical_duplicate_after_failed_decode_is_not_ignored() {
        let slot = 252_424;
        let leader_keypair = Arc::new(Keypair::new());
        let reed_solomon_cache = ReedSolomonCache::default();
        let shredder = Shredder::new(slot, slot - 1, 0, 0).unwrap();
        let entries = make_slot_entries_with_transactions(64);
        let (data_shreds, coding_shreds) = shredder.entries_to_shreds(
            &leader_keypair,
            entries.as_slice(),
            true,
            Some(Hash::new_from_array([8u8; 32])),
            0,
            0,
            true,
            &reed_solomon_cache,
            &mut ProcessShredsStats::default(),
        );
        assert_eq!(data_shreds.len(), 32);
        assert_eq!(coding_shreds.len(), 32);

        let poisoned_index = data_shreds[0].index() as usize;
        let mut poisoned_payload = data_shreds[0].payload().to_vec();
        // Corrupt the first bytes of the serialized Vec<Entry> prefix so call-1 decode fails.
        poisoned_payload[88..96].fill(0xFF);

        // Call 1: ingest all data/coding, but with one poisoned data shred.
        let packets_call1 = data_shreds
            .iter()
            .enumerate()
            .map(|(i, s)| {
                if i == poisoned_index {
                    packet_from_payload(&poisoned_payload)
                } else {
                    packet_from_payload(s.payload())
                }
            })
            .chain(
                coding_shreds
                    .iter()
                    .map(|s| packet_from_payload(s.payload())),
            )
            .collect_vec();

        let metrics = Arc::new(ShredMetrics::default());
        let rs_cache = ReedSolomonCache::default();
        let mut all_shreds = ahash::HashMap::default();
        let mut slot_fec_keys_to_iterate: Vec<(Slot, super::FecSetKey)> = Vec::new();
        let mut deshredded_entries = Vec::new();
        let mut highest_slot_seen = 0;
        let mut scratch = super::ReconstructScratch::default();

        let recovered_count_1 = super::reconstruct_shreds(
            vec![PacketBatch::new(packets_call1)],
            &mut all_shreds,
            &mut slot_fec_keys_to_iterate,
            &mut deshredded_entries,
            &mut highest_slot_seen,
            &rs_cache,
            TEST_RECONSTRUCT_CFG_DECODE_ENTRIES,
            &metrics,
            &mut scratch,
        );
        assert_eq!(recovered_count_1, 0);
        assert!(
            deshredded_entries.is_empty(),
            "poisoned payload should fail call-1 decode"
        );

        // Call 2: deliver the canonical duplicate for the poisoned index.
        let recovered_count_2 = super::reconstruct_shreds(
            vec![PacketBatch::new(vec![packet_from_payload(
                data_shreds[poisoned_index].payload(),
            )])],
            &mut all_shreds,
            &mut slot_fec_keys_to_iterate,
            &mut deshredded_entries,
            &mut highest_slot_seen,
            &rs_cache,
            TEST_RECONSTRUCT_CFG_DECODE_ENTRIES,
            &metrics,
            &mut scratch,
        );

        assert_eq!(recovered_count_2, 0);
        assert_eq!(
            deshredded_entries
                .iter()
                .map(|(_slot, entries, _bytes)| entries.len())
                .sum::<usize>(),
            entries.len(),
            "late canonical duplicate should overwrite poisoned data and decode successfully"
        );
    }

    #[test]
    fn test_known_start_parity_scrub_recovers_poisoned_complete_set() {
        let slot = 252_524;
        let leader_keypair = Arc::new(Keypair::new());
        let reed_solomon_cache = ReedSolomonCache::default();
        let shredder = Shredder::new(slot, slot - 1, 0, 0).unwrap();
        let entries = make_slot_entries_with_transactions(64);
        let (data_shreds, coding_shreds) = shredder.entries_to_shreds(
            &leader_keypair,
            entries.as_slice(),
            true,
            Some(Hash::new_from_array([6u8; 32])),
            0,
            0,
            true,
            &reed_solomon_cache,
            &mut ProcessShredsStats::default(),
        );
        assert_eq!(data_shreds.len(), 32);
        assert_eq!(coding_shreds.len(), 32);

        let poisoned_index = data_shreds[0].index() as usize;
        let mut poisoned_payload = data_shreds[poisoned_index].payload().to_vec();
        poisoned_payload[88..96].fill(0xFF);

        let packets = data_shreds
            .iter()
            .enumerate()
            .map(|(i, s)| {
                if i == poisoned_index {
                    packet_from_payload(&poisoned_payload)
                } else {
                    packet_from_payload(s.payload())
                }
            })
            .chain(
                coding_shreds
                    .iter()
                    .map(|s| packet_from_payload(s.payload())),
            )
            .collect_vec();

        let metrics = Arc::new(ShredMetrics::default());
        let rs_cache = ReedSolomonCache::default();
        let mut all_shreds = ahash::HashMap::default();
        let mut slot_fec_keys_to_iterate: Vec<(Slot, super::FecSetKey)> = Vec::new();
        let mut deshredded_entries = Vec::new();
        let mut highest_slot_seen = 0;
        let mut scratch = super::ReconstructScratch::default();
        let cfg = super::ReconstructShredsConfig {
            known_start_parity_scrub_enabled: true,
            ..TEST_RECONSTRUCT_CFG_DECODE_ENTRIES
        };

        let recovered_count = super::reconstruct_shreds(
            vec![PacketBatch::new(packets)],
            &mut all_shreds,
            &mut slot_fec_keys_to_iterate,
            &mut deshredded_entries,
            &mut highest_slot_seen,
            &rs_cache,
            cfg,
            &metrics,
            &mut scratch,
        );

        assert!(
            recovered_count >= 1,
            "expected known-start parity scrub to recover at least one data shred"
        );
        assert_eq!(
            deshredded_entries
                .iter()
                .map(|(_slot, decoded, _payload)| decoded.len())
                .sum::<usize>(),
            entries.len(),
            "expected successful decode after parity scrub retry"
        );
        assert!(
            metrics
                .known_start_parity_scrub_attempt_count
                .load(Ordering::Relaxed)
                >= 1
        );
        assert_eq!(
            metrics
                .known_start_parity_scrub_success_count
                .load(Ordering::Relaxed),
            1,
            "expected one successful scrub event"
        );
        assert_eq!(
            metrics
                .known_start_parity_scrub_decode_retry_success_count
                .load(Ordering::Relaxed),
            1,
            "expected decode to succeed on scrub retry"
        );
        assert_eq!(
            metrics
                .deshred_error_known_start_count
                .load(Ordering::Relaxed),
            0,
            "successful scrub retry should not count a terminal known-start deshred error"
        );
        assert_eq!(
            metrics
                .bincode_deserialize_error_count
                .load(Ordering::Relaxed),
            0,
            "successful scrub retry should not count a terminal bincode deserialize error"
        );
    }

    #[test]
    fn test_known_start_parity_scrub_budget_resets_after_generation_change() {
        let slot = 252_522;
        let leader_keypair = Arc::new(Keypair::new());
        let reed_solomon_cache = ReedSolomonCache::default();
        let shredder = Shredder::new(slot, slot - 1, 0, 0).unwrap();
        let entries = make_slot_entries_with_transactions(64);
        let (data_shreds, coding_shreds) = shredder.entries_to_shreds(
            &leader_keypair,
            entries.as_slice(),
            true,
            Some(Hash::new_from_array([4u8; 32])),
            0,
            0,
            true,
            &reed_solomon_cache,
            &mut ProcessShredsStats::default(),
        );
        assert_eq!(data_shreds.len(), 32);
        assert_eq!(coding_shreds.len(), 32);

        // Poison a non-boundary index so a single scrub attempt is unlikely to repair.
        let mut poisoned_payloads = data_shreds
            .iter()
            .map(|s| s.payload().to_vec())
            .collect_vec();
        poisoned_payloads[10][88..96].fill(0xFF);

        let packets = data_shreds
            .iter()
            .enumerate()
            .map(|(i, _s)| packet_from_payload(&poisoned_payloads[i]))
            .chain(
                coding_shreds
                    .iter()
                    .map(|s| packet_from_payload(s.payload())),
            )
            .collect_vec();

        let metrics = Arc::new(ShredMetrics::default());
        let rs_cache = ReedSolomonCache::default();
        let mut all_shreds = ahash::HashMap::default();
        let mut slot_fec_keys_to_iterate: Vec<(Slot, super::FecSetKey)> = Vec::new();
        let mut deshredded_entries = Vec::new();
        let mut highest_slot_seen = 0;
        let mut scratch = super::ReconstructScratch::default();

        // Seed tracker state without automatic scrub so we can invoke scrub helper directly.
        let recovered_count = super::reconstruct_shreds(
            vec![PacketBatch::new(packets)],
            &mut all_shreds,
            &mut slot_fec_keys_to_iterate,
            &mut deshredded_entries,
            &mut highest_slot_seen,
            &rs_cache,
            TEST_RECONSTRUCT_CFG_DECODE_ENTRIES,
            &metrics,
            &mut scratch,
        );
        assert_eq!(recovered_count, 0);
        assert!(deshredded_entries.is_empty());

        let mut scrub_scratch = super::ReconstructScratch::default();
        let mut total_recovered = 0usize;
        let cfg = super::ReconstructShredsConfig {
            known_start_parity_scrub_enabled: true,
            known_start_parity_scrub_max_indices_per_fec: 3,
            known_start_parity_scrub_max_attempts_per_fec_generation: 1,
            ..TEST_RECONSTRUCT_CFG_DECODE_ENTRIES
        };
        let start_idx = data_shreds.iter().map(|s| s.index()).min().unwrap() as super::ShredIndex;
        let end_idx = data_shreds.iter().map(|s| s.index()).max().unwrap() as super::ShredIndex;

        let (fec_sets, tracker) = all_shreds
            .get_mut(&slot)
            .expect("slot should be tracked after ingest");
        let target_key = tracker.data_shreds[start_idx as usize]
            .as_ref()
            .map(super::FecSetKey::from_shred)
            .expect("start index should contain a data shred");
        let generation_before = fec_sets
            .get(&target_key)
            .map(|f| f.generation)
            .expect("expected tracked fec state");

        let first = super::try_known_start_parity_scrub(
            slot,
            start_idx,
            end_idx,
            fec_sets,
            tracker,
            &rs_cache,
            cfg,
            &metrics,
            &mut scrub_scratch,
            Instant::now(),
            &mut total_recovered,
        );
        assert!(
            !first,
            "single-attempt scrub should not repair this non-start corruption"
        );
        assert_eq!(
            metrics
                .known_start_parity_scrub_attempt_count
                .load(Ordering::Relaxed),
            1
        );
        assert!(
            metrics
                .known_start_parity_scrub_skip_budget_count
                .load(Ordering::Relaxed)
                >= 1,
            "expected budget skip metric increment"
        );

        // A repeated scrub in the same generation should be blocked.
        let second = super::try_known_start_parity_scrub(
            slot,
            start_idx,
            end_idx,
            fec_sets,
            tracker,
            &rs_cache,
            cfg,
            &metrics,
            &mut scrub_scratch,
            Instant::now(),
            &mut total_recovered,
        );
        assert!(!second);
        assert_eq!(
            metrics
                .known_start_parity_scrub_attempt_count
                .load(Ordering::Relaxed),
            1
        );

        // Overwrite one in-range shred with conflicting payload to bump this FEC generation.
        let replace_idx = (start_idx as usize).saturating_add(1);
        let old_key = tracker.data_shreds[replace_idx]
            .as_ref()
            .map(super::FecSetKey::from_shred);
        let mut conflicting = tracker.data_shreds[replace_idx]
            .clone()
            .expect("replace index should contain a data shred");
        if let Shred::ShredData(ref mut data) = conflicting {
            data.payload[96..104].fill(0xEE);
        } else {
            panic!("expected data shred at replace index");
        }
        assert!(super::ingest_data_shred(
            conflicting,
            tracker,
            super::DataShredSource::Incoming
        ));
        let new_key = tracker.data_shreds[replace_idx]
            .as_ref()
            .map(super::FecSetKey::from_shred);
        super::update_fec_state_after_data_shred_change(fec_sets, old_key, new_key);

        let generation_after = fec_sets
            .get(&target_key)
            .map(|f| f.generation)
            .expect("expected tracked fec state after overwrite");
        assert_ne!(
            generation_after, generation_before,
            "conflicting overwrite should advance FEC generation"
        );

        // Budget should reset for the new generation and allow one more attempt.
        let third = super::try_known_start_parity_scrub(
            slot,
            start_idx,
            end_idx,
            fec_sets,
            tracker,
            &rs_cache,
            cfg,
            &metrics,
            &mut scrub_scratch,
            Instant::now(),
            &mut total_recovered,
        );
        assert!(!third);
        assert_eq!(
            metrics
                .known_start_parity_scrub_attempt_count
                .load(Ordering::Relaxed),
            2,
            "generation bump should reopen one scrub attempt"
        );
    }

    #[test]
    fn test_known_start_scrub_candidate_builder_supports_more_than_three() {
        let mut candidates = Vec::new();
        super::build_known_start_scrub_candidate_indices(10, 31, 6, &mut candidates);
        assert_eq!(candidates.len(), 6);
        assert_eq!(candidates.first().copied(), Some(10));
        assert_eq!(candidates.last().copied(), Some(31));
        assert!(
            candidates.windows(2).all(|w| w[0] < w[1]),
            "expected strictly increasing unique candidate positions"
        );
    }

    #[test]
    fn test_conflicting_coding_duplicate_replacement_retries_recovery() {
        let slot = 252_525;
        let leader_keypair = Arc::new(Keypair::new());
        let reed_solomon_cache = ReedSolomonCache::default();
        let shredder = Shredder::new(slot, slot - 1, 0, 0).unwrap();
        let entries = make_slot_entries_with_transactions(64);
        let (data_shreds, coding_shreds) = shredder.entries_to_shreds(
            &leader_keypair,
            entries.as_slice(),
            true,
            Some(Hash::new_from_array([7u8; 32])),
            0,
            0,
            true,
            &reed_solomon_cache,
            &mut ProcessShredsStats::default(),
        );
        assert_eq!(data_shreds.len(), 32);
        assert_eq!(coding_shreds.len(), 32);

        let missing_data_index = data_shreds[0].index() as usize;
        let canonical_coding = coding_shreds[0].payload().to_vec();
        let mut poisoned_coding = canonical_coding.clone();
        // Corrupt one byte in the coding shard erasure data (offset >= 89 for MerkleCode).
        poisoned_coding[100] ^= 0x5A;

        // Call 1: enough shards to recover, but with corrupted coding shard.
        let packets_call1 = data_shreds
            .iter()
            .filter(|s| s.index() as usize != missing_data_index)
            .map(|s| packet_from_payload(s.payload()))
            .chain(std::iter::once(packet_from_payload(&poisoned_coding)))
            .collect_vec();

        let metrics = Arc::new(ShredMetrics::default());
        let rs_cache = ReedSolomonCache::default();
        let mut all_shreds = ahash::HashMap::default();
        let mut slot_fec_keys_to_iterate: Vec<(Slot, super::FecSetKey)> = Vec::new();
        let mut deshredded_entries = Vec::new();
        let mut highest_slot_seen = 0;
        let mut scratch = super::ReconstructScratch::default();

        let recovered_count_1 = super::reconstruct_shreds(
            vec![PacketBatch::new(packets_call1)],
            &mut all_shreds,
            &mut slot_fec_keys_to_iterate,
            &mut deshredded_entries,
            &mut highest_slot_seen,
            &rs_cache,
            TEST_RECONSTRUCT_CFG_DECODE_ENTRIES,
            &metrics,
            &mut scratch,
        );
        assert_eq!(recovered_count_1, 0);
        assert!(
            deshredded_entries.is_empty(),
            "expected no decode with corrupted coding shard"
        );

        // Call 2: same shard count as call 1, but coding shard payload changed to canonical bytes.
        let recovered_count_2 = super::reconstruct_shreds(
            vec![PacketBatch::new(vec![packet_from_payload(
                &canonical_coding,
            )])],
            &mut all_shreds,
            &mut slot_fec_keys_to_iterate,
            &mut deshredded_entries,
            &mut highest_slot_seen,
            &rs_cache,
            TEST_RECONSTRUCT_CFG_DECODE_ENTRIES,
            &metrics,
            &mut scratch,
        );
        assert!(
            recovered_count_2 >= 1,
            "expected recovery retry after same-count coding replacement"
        );
        assert_eq!(
            deshredded_entries
                .iter()
                .map(|(_slot, e, _bytes)| e.len())
                .sum::<usize>(),
            entries.len(),
            "expected decode after replacing pinned conflicting coding duplicate"
        );
    }

    #[test]
    fn test_recovery_filters_minor_variant_mismatch_and_still_decodes() {
        let slot = 252_526;
        let leader_keypair = Arc::new(Keypair::new());
        let reed_solomon_cache = ReedSolomonCache::default();
        let shredder = Shredder::new(slot, slot - 1, 0, 0).unwrap();
        let entries = make_slot_entries_with_transactions(64);
        let (data_shreds, coding_shreds) = shredder.entries_to_shreds(
            &leader_keypair,
            entries.as_slice(),
            true,
            Some(Hash::new_from_array([10u8; 32])),
            0,
            0,
            true,
            &reed_solomon_cache,
            &mut ProcessShredsStats::default(),
        );
        assert_eq!(data_shreds.len(), 32);
        assert_eq!(coding_shreds.len(), 32);

        let missing_data_index = data_shreds[0].index() as usize;
        let mut mismatched_variant_coding = coding_shreds[0].payload().to_vec();
        let original_variant = mismatched_variant_coding[64];
        let original_proof_size = original_variant & 0x0F;
        assert!(
            original_proof_size > 0,
            "expected proof_size > 0 so we can force a mismatched profile"
        );
        // Force a different proof_size while keeping the coding shard otherwise intact.
        // This creates a mixed-variant FEC input set.
        mismatched_variant_coding[64] = (original_variant & 0xF0) | (original_proof_size - 1);

        let packets = data_shreds
            .iter()
            .filter(|s| s.index() as usize != missing_data_index)
            .map(|s| packet_from_payload(s.payload()))
            .chain(coding_shreds.iter().enumerate().map(|(idx, s)| {
                if idx == 0 {
                    packet_from_payload(&mismatched_variant_coding)
                } else {
                    packet_from_payload(s.payload())
                }
            }))
            .collect_vec();

        let metrics = Arc::new(ShredMetrics::default());
        let rs_cache = ReedSolomonCache::default();
        let mut all_shreds = ahash::HashMap::default();
        let mut slot_fec_keys_to_iterate: Vec<(Slot, super::FecSetKey)> = Vec::new();
        let mut deshredded_entries = Vec::new();
        let mut highest_slot_seen = 0;
        let mut scratch = super::ReconstructScratch::default();

        let recovered_count = super::reconstruct_shreds(
            vec![PacketBatch::new(packets)],
            &mut all_shreds,
            &mut slot_fec_keys_to_iterate,
            &mut deshredded_entries,
            &mut highest_slot_seen,
            &rs_cache,
            TEST_RECONSTRUCT_CFG_DECODE_ENTRIES,
            &metrics,
            &mut scratch,
        );

        assert!(
            recovered_count >= 1,
            "expected recovery to filter the minority mismatched variant and recover missing data",
        );
        assert_eq!(
            deshredded_entries
                .iter()
                .map(|(_slot, e, _bytes)| e.len())
                .sum::<usize>(),
            entries.len(),
            "expected decode to succeed despite one mismatched-variant coding shard"
        );
        assert_eq!(
            metrics.fec_recovery_error_count.load(Ordering::Relaxed),
            0,
            "variant filtering should avoid counting this as a recovery error"
        );
    }

    // -----------------------------------------------------------------------------
    // Additional regression tests (unit/synthetic).
    // -----------------------------------------------------------------------------

    #[test]
    fn test_slot_poisoning_outlier_does_not_drop_legit_slots() {
        // Regression test for slot-poisoning: one shred with a huge slot should not cause us to
        // drop nearby legitimate slots as "old".
        let legit_slot: Slot = 1_000;
        let outlier_slot: Slot = 1_000_000_000;
        let leader_keypair = Arc::new(Keypair::new());
        let reed_solomon_cache = ReedSolomonCache::default();

        let shredder_legit = Shredder::new(legit_slot, legit_slot - 1, 0, 0).unwrap();
        let entries_legit = make_slot_entries_with_transactions(64);
        let (data_legit, _coding_legit) = shredder_legit.entries_to_shreds(
            &leader_keypair,
            entries_legit.as_slice(),
            true,
            Some(Hash::new_from_array([1u8; 32])),
            0,
            0,
            true,
            &reed_solomon_cache,
            &mut ProcessShredsStats::default(),
        );
        assert!(!data_legit.is_empty());

        let shredder_outlier = Shredder::new(outlier_slot, outlier_slot - 1, 0, 0).unwrap();
        let entries_outlier = make_slot_entries_with_transactions(8);
        let (data_outlier, _coding_outlier) = shredder_outlier.entries_to_shreds(
            &leader_keypair,
            entries_outlier.as_slice(),
            true,
            Some(Hash::new_from_array([2u8; 32])),
            0,
            0,
            true,
            &reed_solomon_cache,
            &mut ProcessShredsStats::default(),
        );
        assert!(!data_outlier.is_empty());

        // Put the outlier shred first to maximize the chance of poisoning if the code is wrong.
        let packets = vec![
            packet_from_payload(data_outlier[0].payload()),
            packet_from_payload(data_legit[0 % data_legit.len()].payload()),
            packet_from_payload(data_legit[1 % data_legit.len()].payload()),
            packet_from_payload(data_legit[2 % data_legit.len()].payload()),
        ];

        let rs_cache = ReedSolomonCache::default();
        let metrics = Arc::new(ShredMetrics::default());
        let mut all_shreds = ahash::HashMap::default();
        let mut slot_fec_keys_to_iterate: Vec<(Slot, super::FecSetKey)> = Vec::new();
        let mut deshredded_entries = Vec::new();
        let mut highest_slot_seen = 0;
        let packet_batch = PacketBatch::new(packets);
        let mut scratch = super::ReconstructScratch::default();
        super::reconstruct_shreds(
            vec![packet_batch],
            &mut all_shreds,
            &mut slot_fec_keys_to_iterate,
            &mut deshredded_entries,
            &mut highest_slot_seen,
            &rs_cache,
            TEST_RECONSTRUCT_CFG_DECODE_ENTRIES,
            &metrics,
            &mut scratch,
        );

        assert!(
            all_shreds.contains_key(&legit_slot),
            "legit_slot should not be dropped"
        );
        assert!(
            !all_shreds.contains_key(&outlier_slot),
            "outlier_slot should be rejected by max-future-slot window"
        );
    }

    #[test]
    fn test_ignores_trailing_packet_bytes_for_recovery_and_deshred() {
        let slot = 555_555;
        let leader_keypair = Arc::new(Keypair::new());
        let reed_solomon_cache = ReedSolomonCache::default();
        let shredder = Shredder::new(slot, slot - 1, 0, 0).unwrap();

        let entries = make_slot_entries_with_transactions(64);
        let (data_shreds, coding_shreds) = shredder.entries_to_shreds(
            &leader_keypair,
            entries.as_slice(),
            true,
            Some(Hash::new_from_array([1u8; 32])), // chained_merkle_root
            0,                                     // next_shred_index
            0,                                     // next_code_index
            true,                                  // merkle_variant
            &reed_solomon_cache,
            &mut ProcessShredsStats::default(),
        );
        assert_eq!(data_shreds.len(), 32);
        assert_eq!(coding_shreds.len(), 32);

        // Drop some data and some coding shreds so recovery is required.
        const DROP_DATA: usize = 5;
        const DROP_CODE: usize = 7;
        let missing_data_indices = (0..DROP_DATA).collect::<HashSet<_>>();
        let missing_code_indices = (0..DROP_CODE).collect::<HashSet<_>>();

        // Add trailing packet bytes beyond the shred payload size; the decoder should ignore
        // them per spec.
        const TRAILING_LEN: usize = 4; // fits for both data (1203) and code (1228) within PACKET_DATA_SIZE
        const TRAILING_BYTE: u8 = 0xCC;

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
                let payload = s.payload();
                let mut p = Packet::default();
                p.buffer_mut()[..payload.len()].copy_from_slice(payload);
                p.buffer_mut()[payload.len()..payload.len() + TRAILING_LEN].fill(TRAILING_BYTE);
                p.meta_mut().size = payload.len() + TRAILING_LEN;
                p
            })
            .collect_vec();
        assert!(packets.len() >= 32);

        let metrics = Arc::new(ShredMetrics::default());
        let rs_cache = ReedSolomonCache::default();

        let mut all_shreds = ahash::HashMap::default();
        let mut slot_fec_keys_to_iterate: Vec<(Slot, super::FecSetKey)> = Vec::new();
        let mut deshredded_entries = Vec::new();
        let mut highest_slot_seen = 0;
        let packet_batch = PacketBatch::new(packets);
        let mut scratch = super::ReconstructScratch::default();
        let recovered_count = super::reconstruct_shreds(
            vec![packet_batch],
            &mut all_shreds,
            &mut slot_fec_keys_to_iterate,
            &mut deshredded_entries,
            &mut highest_slot_seen,
            &rs_cache,
            TEST_RECONSTRUCT_CFG_DECODE_ENTRIES,
            &metrics,
            &mut scratch,
        );

        assert_eq!(
            recovered_count, DROP_DATA,
            "expected to recover the missing data shreds even with trailing packet bytes"
        );
        assert_eq!(
            deshredded_entries
                .iter()
                .map(|(_slot, e, _bytes)| e.len())
                .sum::<usize>(),
            entries.len()
        );
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
            Some(Hash::new_from_array([1u8; 32])), // chained_merkle_root
            0,                                     // next_shred_index
            0,                                     // next_code_index
            true,                                  // merkle_variant
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
        let mut slot_fec_keys_to_iterate: Vec<(Slot, super::FecSetKey)> = Vec::new();
        let mut deshredded_entries = Vec::new();
        let mut highest_slot_seen = 0;
        let packet_batch = PacketBatch::new(packets);
        let mut scratch = super::ReconstructScratch::default();
        let recovered_count = super::reconstruct_shreds(
            vec![packet_batch],
            &mut all_shreds,
            &mut slot_fec_keys_to_iterate,
            &mut deshredded_entries,
            &mut highest_slot_seen,
            &rs_cache,
            TEST_RECONSTRUCT_CFG_DECODE_ENTRIES,
            &metrics,
            &mut scratch,
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
    fn test_accepts_non_zero_trailing_bytes_when_boundary_known() {
        let slot = 444_444;
        let leader_keypair = Arc::new(Keypair::new());
        let reed_solomon_cache = ReedSolomonCache::default();
        let shredder = Shredder::new(slot, slot - 1, 0, 0).unwrap();

        let entries = make_slot_entries_with_transactions(8);
        let (data_shreds, _coding_shreds) = shredder.entries_to_shreds(
            &leader_keypair,
            entries.as_slice(),
            false,
            Some(Hash::new_from_array([1u8; 32])), // chained_merkle_root
            0,                                     // next_shred_index
            0,                                     // next_code_index
            true,                                  // merkle_variant
            &reed_solomon_cache,
            &mut ProcessShredsStats::default(),
        );
        assert!(!data_shreds.is_empty());

        let last_data_shred = data_shreds
            .iter()
            .filter(|s| s.is_data())
            .max_by_key(|s| s.index())
            .expect("must have at least one data shred");

        // Append non-zero trailing bytes within the allowed data buffer by increasing the `size`
        // field in the last data shred payload.
        let mut last_payload = last_data_shred.payload().to_vec();
        let variant = solana_ledger::shred::layout::get_shred_variant(&last_payload).unwrap();
        let max_size = match variant {
            solana_ledger::shred::ShredVariant::MerkleData {
                proof_size,
                chained,
                resigned,
            } => {
                let cap = solana_ledger::shred::ShredData::capacity(Some((
                    proof_size, chained, resigned,
                )))
                .unwrap();
                88 + cap // SIZE_OF_DATA_HEADERS (88) + capacity
            }
            _ => panic!("expected MerkleData shred variant"),
        };
        let orig_size = u16::from_le_bytes(last_payload[86..88].try_into().unwrap()) as usize;
        let new_size = std::cmp::min(orig_size + 8, max_size);
        assert!(
            new_size > orig_size,
            "last shred already at max size; can't append trailing bytes"
        );
        last_payload[86..88].copy_from_slice(&(new_size as u16).to_le_bytes());
        last_payload[orig_size..new_size].fill(0xAB);

        let packets = data_shreds
            .iter()
            .map(|s| {
                let bytes = if s.index() == last_data_shred.index() {
                    last_payload.as_slice()
                } else {
                    s.payload()
                };
                let mut p = Packet::default();
                p.buffer_mut()[..bytes.len()].copy_from_slice(bytes);
                p.meta_mut().size = bytes.len();
                p
            })
            .collect_vec();

        let metrics = Arc::new(ShredMetrics::default());
        let rs_cache = ReedSolomonCache::default();

        let mut all_shreds = ahash::HashMap::default();
        let mut slot_fec_keys_to_iterate: Vec<(Slot, super::FecSetKey)> = Vec::new();
        let mut deshredded_entries = Vec::new();
        let mut highest_slot_seen = 0;
        let packet_batch = PacketBatch::new(packets);
        let mut scratch = super::ReconstructScratch::default();
        let recovered_count = super::reconstruct_shreds(
            vec![packet_batch],
            &mut all_shreds,
            &mut slot_fec_keys_to_iterate,
            &mut deshredded_entries,
            &mut highest_slot_seen,
            &rs_cache,
            TEST_RECONSTRUCT_CFG_DECODE_ENTRIES,
            &metrics,
            &mut scratch,
        );
        assert_eq!(recovered_count, 0);
        assert_eq!(deshredded_entries.len(), 1);
        assert_eq!(deshredded_entries[0].0, slot);
        assert_eq!(deshredded_entries[0].1.len(), entries.len());

        let decoded_bytes = &deshredded_entries[0].2;
        assert!(decoded_bytes.len() >= 8);
        assert!(
            decoded_bytes[decoded_bytes.len() - 8..]
                .iter()
                .all(|&b| b == 0xAB),
            "expected non-zero trailing bytes appended to deshredded payload"
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
            Some(Hash::new_from_array([1u8; 32])), // chained_merkle_root
            0,                                     // next_shred_index
            0,                                     // next_code_index
            true,                                  // merkle_variant
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
        let mut slot_fec_keys_to_iterate: Vec<(Slot, super::FecSetKey)> = Vec::new();
        let mut deshredded_entries = Vec::new();
        let mut highest_slot_seen = 0;
        let packet_batch = PacketBatch::new(packets);
        let mut scratch = super::ReconstructScratch::default();
        let recovered_count = super::reconstruct_shreds(
            vec![packet_batch],
            &mut all_shreds,
            &mut slot_fec_keys_to_iterate,
            &mut deshredded_entries,
            &mut highest_slot_seen,
            &rs_cache,
            TEST_RECONSTRUCT_CFG_DECODE_ENTRIES,
            &metrics,
            &mut scratch,
        );

        assert_eq!(recovered_count, 0);
        let (_fec_sets, tracker) = all_shreds.get(&slot).expect("slot entry should exist");
        assert!(
            tracker.already_recovered_fec_sets[0],
            "expected fec_set_index=0 to be marked complete once all data shreds are present"
        );
    }

    #[test]
    fn test_matching_data_count_requires_exact_fec_identity() {
        let (tracker, key, num_data_shreds, _mismatched_idx) =
            build_tracker_with_mismatched_fec_data(333_334);
        let matching = super::matching_data_count_for_fec_key(&tracker, &key, num_data_shreds);
        assert_eq!(
            matching as usize,
            usize::from(num_data_shreds) - 1,
            "data_count must exclude data shreds whose fec_set_index mismatches the key"
        );
    }

    #[test]
    fn test_has_all_data_shreds_requires_exact_fec_identity() {
        let (tracker, key, num_data_shreds, mismatched_idx) =
            build_tracker_with_mismatched_fec_data(333_335);
        assert!(
            !super::has_all_data_shreds_for_fec_set(&tracker, &key, num_data_shreds),
            "FEC completion must not treat mismatched-fec_set_index data as present"
        );
        assert_eq!(
            tracker.data_shreds[mismatched_idx]
                .as_ref()
                .unwrap()
                .fec_set_index(),
            key.fec_set_index + 1
        );
    }

    #[test]
    fn test_unknown_start_decode_does_not_commit_and_can_later_commit_without_duplicate_emit() {
        // Build two independent data sets in the same slot. Use tick entries because unknown-start
        // decodes are gated on basic entry hash-chain sanity.
        let slot: Slot = 222_000;
        let leader_keypair = Arc::new(Keypair::new());
        let reed_solomon_cache = ReedSolomonCache::default();
        let thread_pool = solana_entry::entry::thread_pool_for_tests();

        let entries1 = solana_entry::entry::create_ticks(
            8,                               // num_ticks
            1,                               // hashes_per_tick
            Hash::new_from_array([9u8; 32]), // start hash
        );
        let bytes1 = bincode::serialize(&entries1).unwrap();
        let shreds1 = solana_ledger::shred::merkle::make_shreds_from_data(
            &thread_pool,
            &leader_keypair,
            Some(Hash::new_from_array([3u8; 32])),
            &bytes1,
            slot,
            slot - 1,
            0,     // shred_version
            0,     // reference_tick
            false, // is_last_in_slot
            0,     // next_shred_index
            0,     // next_code_index
            &reed_solomon_cache,
            &mut ProcessShredsStats::default(),
        )
        .unwrap();
        let data1 = shreds1
            .into_iter()
            .filter(|s| matches!(s, Shred::ShredData(_)))
            .collect_vec();
        assert!(!data1.is_empty());
        let end1 = data1.iter().map(|s| s.index()).max().unwrap();

        let entries2 = solana_entry::entry::create_ticks(8, 1, entries1.last().unwrap().hash);
        let bytes2 = bincode::serialize(&entries2).unwrap();
        let shreds2 = solana_ledger::shred::merkle::make_shreds_from_data(
            &thread_pool,
            &leader_keypair,
            Some(Hash::new_from_array([4u8; 32])),
            &bytes2,
            slot,
            slot - 1,
            0,    // shred_version
            0,    // reference_tick
            true, // is_last_in_slot
            end1 + 1,
            end1 + 1,
            &reed_solomon_cache,
            &mut ProcessShredsStats::default(),
        )
        .unwrap();
        let data2 = shreds2
            .into_iter()
            .filter(|s| matches!(s, Shred::ShredData(_)))
            .collect_vec();
        assert!(!data2.is_empty());

        let start2 = data2
            .iter()
            .map(|s| s.index() as super::ShredIndex)
            .min()
            .unwrap();
        let end2 = data2
            .iter()
            .map(|s| s.index() as super::ShredIndex)
            .max()
            .unwrap();
        assert_eq!(
            start2 as usize,
            (end1 + 1) as usize,
            "expected second set to start right after the first set"
        );

        // Ensure the second set is decodable and would pass the unknown-start gates (trailing
        // bytes must be all zeros; basic entry hash-chain sanity).
        let payloads2 = data2
            .iter()
            .sorted_by_key(|s| s.index())
            .map(|s| s.payload())
            .collect_vec();
        let deshredded2 = Shredder::deshred(payloads2).unwrap();
        let bincode_opts = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .with_limit(BINCODE_DESERIALIZE_LIMIT_BYTES as u64);
        let mut cursor = Cursor::new(&deshredded2);
        let decoded2 = bincode_opts
            .allow_trailing_bytes()
            .deserialize_from::<_, Vec<solana_entry::entry::Entry>>(&mut cursor)
            .unwrap();
        let consumed = cursor.position() as usize;
        let trailing = &deshredded2[consumed..];
        assert!(
            trailing.iter().all(|&b| b == 0),
            "expected unknown-start trailing bytes to be all zeros"
        );
        assert!(
            super::entries_pass_basic_sanity(&decoded2),
            "expected generated entries to pass basic sanity"
        );

        // Call 1: omit the boundary shred (end1) so decoding the second set uses unknown_start.
        let mut packets_call1 = Vec::new();
        // Include all shreds from the first set except its terminal DATA_COMPLETE shred.
        for s in data1.iter().filter(|s| s.index() != end1) {
            packets_call1.push(packet_from_payload(s.payload()));
        }
        // Include the entire second set.
        for s in &data2 {
            packets_call1.push(packet_from_payload(s.payload()));
        }

        let rs_cache = ReedSolomonCache::default();
        let metrics = Arc::new(ShredMetrics::default());
        let mut all_shreds = ahash::HashMap::default();
        let mut slot_fec_keys_to_iterate: Vec<(Slot, super::FecSetKey)> = Vec::new();
        let mut deshredded_entries = Vec::new();
        let mut highest_slot_seen = 0;
        let packet_batch = PacketBatch::new(packets_call1);
        let mut scratch = super::ReconstructScratch::default();
        super::reconstruct_shreds(
            vec![packet_batch],
            &mut all_shreds,
            &mut slot_fec_keys_to_iterate,
            &mut deshredded_entries,
            &mut highest_slot_seen,
            &rs_cache,
            TEST_RECONSTRUCT_CFG_DECODE_ENTRIES,
            &metrics,
            &mut scratch,
        );

        assert_eq!(deshredded_entries.len(), 1, "should decode second set once");
        let tracker = &all_shreds.get(&slot).unwrap().1;
        assert_eq!(
            tracker
                .unknown_start_emitted
                .get(&end2)
                .map(|emitted| emitted.start_data_complete_idx),
            Some(start2)
        );
        assert!(
            !tracker.already_deshredded[start2 as usize],
            "unknown-start decode must not mark indices as consumed"
        );
        assert!(
            tracker.data_shreds[start2 as usize].is_some(),
            "unknown-start decode must not drop payloads"
        );
        assert!(
            !tracker.already_deshredded[end2 as usize],
            "unknown-start decode must not mark indices as consumed"
        );
        assert!(
            tracker.data_shreds[end2 as usize].is_some(),
            "unknown-start decode must not drop payloads"
        );

        // Call 2: provide the missing boundary shred. This should allow committing the
        // second range without re-emitting it, and should also decode+commit the first set.
        let boundary = data1.iter().find(|s| s.index() == end1).unwrap();
        let packets_call2 = vec![packet_from_payload(boundary.payload())];
        let packet_batch = PacketBatch::new(packets_call2);
        let mut scratch = super::ReconstructScratch::default();
        super::reconstruct_shreds(
            vec![packet_batch],
            &mut all_shreds,
            &mut slot_fec_keys_to_iterate,
            &mut deshredded_entries,
            &mut highest_slot_seen,
            &rs_cache,
            TEST_RECONSTRUCT_CFG_DECODE_ENTRIES,
            &metrics,
            &mut scratch,
        );

        assert_eq!(
            deshredded_entries.len(),
            1,
            "should decode first set, but not re-emit second set"
        );

        let tracker = &all_shreds.get(&slot).unwrap().1;
        assert!(
            !tracker.unknown_start_emitted.contains_key(&end2),
            "unknown-start emission record should be cleared on commit"
        );
        assert!(
            tracker.already_deshredded[start2 as usize],
            "second-set range should be committed after boundary arrives"
        );
        assert!(
            tracker.already_deshredded[end2 as usize],
            "second-set range should be committed after boundary arrives"
        );
    }

    #[test]
    fn test_unknown_start_accepts_non_zero_trailing_bytes() {
        let slot: Slot = 223_000;
        let leader_keypair = Arc::new(Keypair::new());
        let reed_solomon_cache = ReedSolomonCache::default();
        let thread_pool = solana_entry::entry::thread_pool_for_tests();

        let entries1 = solana_entry::entry::create_ticks(8, 1, Hash::new_from_array([9u8; 32]));
        let bytes1 = bincode::serialize(&entries1).unwrap();
        let shreds1 = solana_ledger::shred::merkle::make_shreds_from_data(
            &thread_pool,
            &leader_keypair,
            Some(Hash::new_from_array([3u8; 32])),
            &bytes1,
            slot,
            slot - 1,
            0,
            0,
            false,
            0,
            0,
            &reed_solomon_cache,
            &mut ProcessShredsStats::default(),
        )
        .unwrap();
        let data1 = shreds1
            .into_iter()
            .filter(|s| matches!(s, Shred::ShredData(_)))
            .collect_vec();
        assert!(!data1.is_empty());
        let end1 = data1.iter().map(|s| s.index()).max().unwrap();

        let entries2 = solana_entry::entry::create_ticks(8, 1, entries1.last().unwrap().hash);
        let bytes2 = bincode::serialize(&entries2).unwrap();
        let shreds2 = solana_ledger::shred::merkle::make_shreds_from_data(
            &thread_pool,
            &leader_keypair,
            Some(Hash::new_from_array([4u8; 32])),
            &bytes2,
            slot,
            slot - 1,
            0,
            0,
            true,
            end1 + 1,
            end1 + 1,
            &reed_solomon_cache,
            &mut ProcessShredsStats::default(),
        )
        .unwrap();
        let data2 = shreds2
            .into_iter()
            .filter(|s| matches!(s, Shred::ShredData(_)))
            .collect_vec();
        assert!(!data2.is_empty());

        let start2 = data2
            .iter()
            .map(|s| s.index() as super::ShredIndex)
            .min()
            .unwrap();
        let end2 = data2
            .iter()
            .map(|s| s.index() as super::ShredIndex)
            .max()
            .unwrap();
        let last_data_shred_2 = data2
            .iter()
            .max_by_key(|s| s.index())
            .expect("must have at least one data shred in set 2");
        let mut last_payload_2 = last_data_shred_2.payload().to_vec();
        let variant = solana_ledger::shred::layout::get_shred_variant(&last_payload_2).unwrap();
        let max_size = match variant {
            solana_ledger::shred::ShredVariant::MerkleData {
                proof_size,
                chained,
                resigned,
            } => {
                let cap = solana_ledger::shred::ShredData::capacity(Some((
                    proof_size, chained, resigned,
                )))
                .unwrap();
                88 + cap
            }
            _ => panic!("expected MerkleData shred variant"),
        };
        let orig_size = u16::from_le_bytes(last_payload_2[86..88].try_into().unwrap()) as usize;
        let new_size = std::cmp::min(orig_size + 8, max_size);
        assert!(
            new_size > orig_size,
            "last shred already at max size; can't append trailing bytes"
        );
        last_payload_2[86..88].copy_from_slice(&(new_size as u16).to_le_bytes());
        last_payload_2[orig_size..new_size].fill(0xAB);

        // Omit the first set boundary so set 2 decodes via unknown-start path.
        let mut packets_call1 = Vec::new();
        for s in data1.iter().filter(|s| s.index() != end1) {
            packets_call1.push(packet_from_payload(s.payload()));
        }
        for s in &data2 {
            if s.index() == last_data_shred_2.index() {
                packets_call1.push(packet_from_payload(&last_payload_2));
            } else {
                packets_call1.push(packet_from_payload(s.payload()));
            }
        }

        let rs_cache = ReedSolomonCache::default();
        let metrics = Arc::new(ShredMetrics::default());
        let mut all_shreds = ahash::HashMap::default();
        let mut slot_fec_keys_to_iterate: Vec<(Slot, super::FecSetKey)> = Vec::new();
        let mut deshredded_entries = Vec::new();
        let mut highest_slot_seen = 0;
        let mut scratch = super::ReconstructScratch::default();
        super::reconstruct_shreds(
            vec![PacketBatch::new(packets_call1)],
            &mut all_shreds,
            &mut slot_fec_keys_to_iterate,
            &mut deshredded_entries,
            &mut highest_slot_seen,
            &rs_cache,
            TEST_RECONSTRUCT_CFG_DECODE_ENTRIES,
            &metrics,
            &mut scratch,
        );

        assert_eq!(deshredded_entries.len(), 1, "should decode second set once");
        assert_eq!(deshredded_entries[0].1.len(), entries2.len());
        assert!(
            deshredded_entries[0].2.ends_with(&[0xAB; 8]),
            "expected non-zero trailing bytes to be preserved in unknown-start decode"
        );

        let tracker = &all_shreds.get(&slot).unwrap().1;
        assert_eq!(
            tracker
                .unknown_start_emitted
                .get(&end2)
                .map(|emitted| emitted.start_data_complete_idx),
            Some(start2)
        );
        assert_eq!(
            metrics
                .fec_set_decode_unknown_start_latency_count
                .load(Ordering::Relaxed),
            1,
            "expected one unknown-start completion latency sample"
        );
        assert_eq!(
            metrics
                .fec_set_decode_known_start_latency_count
                .load(Ordering::Relaxed),
            0,
            "expected no known-start completion latency samples"
        );
    }

    #[test]
    fn test_unknown_start_canonical_commit_redecodes_after_data_change() {
        let slot: Slot = 223_500;
        let leader_keypair = Arc::new(Keypair::new());
        let reed_solomon_cache = ReedSolomonCache::default();
        let thread_pool = solana_entry::entry::thread_pool_for_tests();

        let entries1 = solana_entry::entry::create_ticks(8, 1, Hash::new_from_array([9u8; 32]));
        let bytes1 = bincode::serialize(&entries1).unwrap();
        let shreds1 = solana_ledger::shred::merkle::make_shreds_from_data(
            &thread_pool,
            &leader_keypair,
            Some(Hash::new_from_array([3u8; 32])),
            &bytes1,
            slot,
            slot - 1,
            0,
            0,
            false,
            0,
            0,
            &reed_solomon_cache,
            &mut ProcessShredsStats::default(),
        )
        .unwrap();
        let data1 = shreds1
            .into_iter()
            .filter(|s| matches!(s, Shred::ShredData(_)))
            .collect_vec();
        assert!(!data1.is_empty());
        let end1 = data1.iter().map(|s| s.index()).max().unwrap();

        let entries2 = solana_entry::entry::create_ticks(8, 1, entries1.last().unwrap().hash);
        let bytes2 = bincode::serialize(&entries2).unwrap();
        let shreds2 = solana_ledger::shred::merkle::make_shreds_from_data(
            &thread_pool,
            &leader_keypair,
            Some(Hash::new_from_array([4u8; 32])),
            &bytes2,
            slot,
            slot - 1,
            0,
            0,
            true,
            end1 + 1,
            end1 + 1,
            &reed_solomon_cache,
            &mut ProcessShredsStats::default(),
        )
        .unwrap();
        let data2 = shreds2
            .into_iter()
            .filter(|s| matches!(s, Shred::ShredData(_)))
            .collect_vec();
        assert!(!data2.is_empty());

        let start2 = data2
            .iter()
            .map(|s| s.index() as super::ShredIndex)
            .min()
            .unwrap();
        let end2 = data2
            .iter()
            .map(|s| s.index() as super::ShredIndex)
            .max()
            .unwrap();
        let last_data_shred_2 = data2
            .iter()
            .max_by_key(|s| s.index())
            .expect("must have terminal data shred in set 2");

        // Call 1: omit set-1 boundary so set 2 is emitted via unknown-start.
        let mut packets_call1 = Vec::new();
        for s in data1.iter().filter(|s| s.index() != end1) {
            packets_call1.push(packet_from_payload(s.payload()));
        }
        for s in &data2 {
            packets_call1.push(packet_from_payload(s.payload()));
        }

        let rs_cache = ReedSolomonCache::default();
        let metrics = Arc::new(ShredMetrics::default());
        let mut all_shreds = ahash::HashMap::default();
        let mut slot_fec_keys_to_iterate: Vec<(Slot, super::FecSetKey)> = Vec::new();
        let mut deshredded_entries = Vec::new();
        let mut highest_slot_seen = 0;
        let mut scratch = super::ReconstructScratch::default();
        super::reconstruct_shreds(
            vec![PacketBatch::new(packets_call1)],
            &mut all_shreds,
            &mut slot_fec_keys_to_iterate,
            &mut deshredded_entries,
            &mut highest_slot_seen,
            &rs_cache,
            TEST_RECONSTRUCT_CFG_DECODE_ENTRIES,
            &metrics,
            &mut scratch,
        );
        assert_eq!(
            deshredded_entries.len(),
            1,
            "expected unknown-start emit for set 2"
        );
        let emitted_range_generation = {
            let tracker = &all_shreds.get(&slot).unwrap().1;
            let emitted = tracker
                .unknown_start_emitted
                .get(&end2)
                .expect("unknown-start emission must be tracked");
            assert_eq!(emitted.start_data_complete_idx, start2);
            emitted.range_generation
        };

        // Mutate a duplicate shred in set 2 (valid size growth with non-zero trailing bytes).
        let mut changed_payload = last_data_shred_2.payload().to_vec();
        let variant = solana_ledger::shred::layout::get_shred_variant(&changed_payload).unwrap();
        let max_size = match variant {
            solana_ledger::shred::ShredVariant::MerkleData {
                proof_size,
                chained,
                resigned,
            } => {
                let cap = solana_ledger::shred::ShredData::capacity(Some((
                    proof_size, chained, resigned,
                )))
                .unwrap();
                88 + cap
            }
            _ => panic!("expected MerkleData shred variant"),
        };
        let orig_size = u16::from_le_bytes(changed_payload[86..88].try_into().unwrap()) as usize;
        let new_size = std::cmp::min(orig_size + 8, max_size);
        assert!(
            new_size > orig_size,
            "last shred already at max size; can't append trailing bytes"
        );
        changed_payload[86..88].copy_from_slice(&(new_size as u16).to_le_bytes());
        changed_payload[orig_size..new_size].fill(0xCD);

        // Build expected canonical bytes for set 2 after the duplicate overwrite.
        let expected_set2_payload_after_change = {
            let payloads = data2
                .iter()
                .sorted_by_key(|s| s.index())
                .map(|s| {
                    if s.index() == last_data_shred_2.index() {
                        changed_payload.as_slice()
                    } else {
                        s.payload()
                    }
                })
                .collect_vec();
            Shredder::deshred(payloads).unwrap()
        };

        // Call 2: ingest changed duplicate while boundary is still unknown. This must not emit.
        let mut scratch = super::ReconstructScratch::default();
        super::reconstruct_shreds(
            vec![PacketBatch::new(vec![packet_from_payload(
                &changed_payload,
            )])],
            &mut all_shreds,
            &mut slot_fec_keys_to_iterate,
            &mut deshredded_entries,
            &mut highest_slot_seen,
            &rs_cache,
            TEST_RECONSTRUCT_CFG_DECODE_ENTRIES,
            &metrics,
            &mut scratch,
        );
        assert!(
            deshredded_entries.is_empty(),
            "should not re-emit unknown-start decode while boundary remains missing"
        );
        let tracker = &all_shreds.get(&slot).unwrap().1;
        let updated_emitted = tracker
            .unknown_start_emitted
            .get(&end2)
            .expect("unknown-start emission should remain tracked");
        assert_eq!(updated_emitted.start_data_complete_idx, start2);
        assert_eq!(
            updated_emitted.range_generation, emitted_range_generation,
            "tracked emission generation should remain at original emit generation"
        );
        assert!(
            tracker.range_generation_max(start2, end2) > emitted_range_generation,
            "tracked range generation should advance after in-range duplicate overwrite"
        );

        // Call 3: provide the missing boundary. Known-start commit must re-decode set 2 because
        // payload bytes changed after provisional emission.
        let boundary = data1
            .iter()
            .find(|s| s.index() == end1)
            .expect("set-1 terminal boundary shred must exist");
        let mut scratch = super::ReconstructScratch::default();
        super::reconstruct_shreds(
            vec![PacketBatch::new(vec![packet_from_payload(
                boundary.payload(),
            )])],
            &mut all_shreds,
            &mut slot_fec_keys_to_iterate,
            &mut deshredded_entries,
            &mut highest_slot_seen,
            &rs_cache,
            TEST_RECONSTRUCT_CFG_DECODE_ENTRIES,
            &metrics,
            &mut scratch,
        );

        assert_eq!(
            deshredded_entries.len(),
            2,
            "expected first-set decode plus corrected re-decode of set 2"
        );
        assert!(
            deshredded_entries
                .iter()
                .any(|(decoded_slot, _entries, bytes)| {
                    *decoded_slot == slot && *bytes == expected_set2_payload_after_change
                }),
            "expected call-3 output to include changed canonical bytes for set 2"
        );

        let tracker = &all_shreds.get(&slot).unwrap().1;
        assert!(
            !tracker.unknown_start_emitted.contains_key(&end2),
            "unknown-start emission record should clear once known-start commit happens"
        );
        assert!(tracker.already_deshredded[start2 as usize]);
        assert!(tracker.already_deshredded[end2 as usize]);
    }

    #[test]
    fn test_unknown_start_can_fallback_to_later_candidate_start() {
        let slot: Slot = 224_000;
        let leader_keypair = Arc::new(Keypair::new());
        let reed_solomon_cache = ReedSolomonCache::default();
        let thread_pool = solana_entry::entry::thread_pool_for_tests();

        let entries1 = solana_entry::entry::create_ticks(8, 1, Hash::new_from_array([9u8; 32]));
        let bytes1 = bincode::serialize(&entries1).unwrap();
        let shreds1 = solana_ledger::shred::merkle::make_shreds_from_data(
            &thread_pool,
            &leader_keypair,
            Some(Hash::new_from_array([3u8; 32])),
            &bytes1,
            slot,
            slot - 1,
            0,
            0,
            false,
            0,
            0,
            &reed_solomon_cache,
            &mut ProcessShredsStats::default(),
        )
        .unwrap();
        let data1 = shreds1
            .into_iter()
            .filter(|s| matches!(s, Shred::ShredData(_)))
            .collect_vec();
        assert!(!data1.is_empty());
        let end1 = data1.iter().map(|s| s.index()).max().unwrap();

        let entries2 = solana_entry::entry::create_ticks(8, 1, entries1.last().unwrap().hash);
        let bytes2 = bincode::serialize(&entries2).unwrap();
        let shreds2 = solana_ledger::shred::merkle::make_shreds_from_data(
            &thread_pool,
            &leader_keypair,
            Some(Hash::new_from_array([4u8; 32])),
            &bytes2,
            slot,
            slot - 1,
            0,
            0,
            false,
            end1 + 1,
            end1 + 1,
            &reed_solomon_cache,
            &mut ProcessShredsStats::default(),
        )
        .unwrap();
        let data2 = shreds2
            .into_iter()
            .filter(|s| matches!(s, Shred::ShredData(_)))
            .collect_vec();
        assert!(!data2.is_empty());

        let start2 = data2
            .iter()
            .map(|s| s.index() as super::ShredIndex)
            .min()
            .unwrap();
        let end2 = data2
            .iter()
            .map(|s| s.index() as super::ShredIndex)
            .max()
            .unwrap();
        assert_eq!(start2 as usize, (end1 + 1) as usize);

        let entries3 = solana_entry::entry::create_ticks(8, 1, entries2.last().unwrap().hash);
        let bytes3 = bincode::serialize(&entries3).unwrap();
        let shreds3 = solana_ledger::shred::merkle::make_shreds_from_data(
            &thread_pool,
            &leader_keypair,
            Some(Hash::new_from_array([5u8; 32])),
            &bytes3,
            slot,
            slot - 1,
            0,
            0,
            true,
            end2 as u32 + 1,
            end2 as u32 + 1,
            &reed_solomon_cache,
            &mut ProcessShredsStats::default(),
        )
        .unwrap();
        let data3 = shreds3
            .into_iter()
            .filter(|s| matches!(s, Shred::ShredData(_)))
            .collect_vec();
        assert!(!data3.is_empty());
        let start3 = data3
            .iter()
            .map(|s| s.index() as super::ShredIndex)
            .min()
            .unwrap();
        let end3 = data3
            .iter()
            .map(|s| s.index() as super::ShredIndex)
            .max()
            .unwrap();
        assert_eq!(start3 as usize, (end2 as usize) + 1);

        // Omit the set-1 boundary shred to create a gap before set 2.
        let missing_idx = end1;
        let mut packets = Vec::new();
        for s in &data1 {
            if s.index() == missing_idx {
                continue;
            }
            packets.push(packet_from_payload(s.payload()));
        }
        for s in &data2 {
            let mut payload = s.payload().to_vec();
            assert!(
                payload.len() >= 96,
                "expected Merkle data payload to include data region"
            );
            // Poison all set-2 shreds so any candidate that starts in set 2 fails decode.
            payload[88..96].fill(0xFF);
            if s.index() as super::ShredIndex == end2 {
                // Clear DATA_COMPLETE/LAST bits so end2 is not treated as a known boundary.
                payload[85] &= 0x3F;
                assert_eq!(payload[85] & 0xC0, 0);
            }
            packets.push(packet_from_payload(&payload));
        }
        for s in &data3 {
            packets.push(packet_from_payload(s.payload()));
        }

        let fallback_attempts = (start3 - start2 + 1) as usize;
        assert!(
            fallback_attempts >= 2,
            "expected at least one later unknown-start candidate to try"
        );

        let run_case = |unknown_start_max_positions: u16| -> (
            ahash::HashMap<
                Slot,
                (
                    ahash::HashMap<super::FecSetKey, super::FecSetState>,
                    super::ShredsStateTracker,
                ),
            >,
            Vec<(Slot, Vec<solana_entry::entry::Entry>, Vec<u8>)>,
            Arc<ShredMetrics>,
        ) {
            let rs_cache = ReedSolomonCache::default();
            let metrics = Arc::new(ShredMetrics::default());
            let mut all_shreds = ahash::HashMap::default();
            let mut slot_fec_keys_to_iterate: Vec<(Slot, super::FecSetKey)> = Vec::new();
            let mut deshredded_entries = Vec::new();
            let mut highest_slot_seen = 0;
            let mut scratch = super::ReconstructScratch::default();
            let cfg = super::ReconstructShredsConfig {
                unknown_start_max_positions,
                ..TEST_RECONSTRUCT_CFG_DECODE_ENTRIES
            };
            super::reconstruct_shreds(
                vec![PacketBatch::new(packets.clone())],
                &mut all_shreds,
                &mut slot_fec_keys_to_iterate,
                &mut deshredded_entries,
                &mut highest_slot_seen,
                &rs_cache,
                cfg,
                &metrics,
                &mut scratch,
            );
            (all_shreds, deshredded_entries, metrics)
        };

        // With the FEC-aligned candidate fix, the DC shred at end3 has
        // fec_set_index == start3, which is prepended as the first candidate.
        // This skips the poisoned set-2 region entirely, so even max_positions=1
        // succeeds immediately.
        let (all_shreds_single, deshredded_single, metrics_single) = run_case(1);
        assert_eq!(
            deshredded_single.len(),
            1,
            "FEC-aligned candidate should bypass poisoned region and succeed"
        );
        assert_eq!(
            metrics_single
                .unknown_start_position_count
                .load(Ordering::Relaxed),
            1,
            "expected exactly one unknown-start candidate attempt (FEC-aligned)"
        );
        assert_eq!(
            metrics_single
                .unknown_start_position_error_count
                .load(Ordering::Relaxed),
            0,
            "FEC-aligned candidate should succeed without errors"
        );
        let tracker_single = &all_shreds_single.get(&slot).unwrap().1;
        assert_eq!(
            tracker_single
                .unknown_start_emitted
                .get(&end3)
                .map(|emitted| emitted.start_data_complete_idx),
            Some(start3),
            "FEC-aligned decode should emit from the correct start position"
        );

        // With more positions available, FEC-aligned candidate still succeeds on
        // the first try, short-circuiting the linear fallback.
        let (all_shreds_multi, deshredded_multi, metrics_multi) = run_case(
            fallback_attempts
                .try_into()
                .expect("fallback attempts should fit into u16"),
        );
        assert_eq!(
            deshredded_multi.len(),
            1,
            "FEC-aligned candidate should recover decode output"
        );
        assert_eq!(
            metrics_multi
                .unknown_start_position_count
                .load(Ordering::Relaxed),
            1,
            "FEC-aligned candidate should succeed on first attempt, no fallbacks needed"
        );
        assert_eq!(
            metrics_multi
                .unknown_start_position_error_count
                .load(Ordering::Relaxed),
            0,
            "FEC-aligned candidate should succeed without errors"
        );
        let tracker = &all_shreds_multi.get(&slot).unwrap().1;
        assert_eq!(
            tracker
                .unknown_start_emitted
                .get(&end3)
                .map(|emitted| emitted.start_data_complete_idx),
            Some(start3),
            "FEC-aligned decode should emit from the correct start position"
        );
    }

    #[test]
    fn test_unknown_start_skips_retry_when_range_unchanged() {
        let slot: Slot = 224_500;
        let leader_keypair = Arc::new(Keypair::new());
        let reed_solomon_cache = ReedSolomonCache::default();
        let thread_pool = solana_entry::entry::thread_pool_for_tests();

        let entries1 = solana_entry::entry::create_ticks(8, 1, Hash::new_from_array([9u8; 32]));
        let bytes1 = bincode::serialize(&entries1).unwrap();
        let shreds1 = solana_ledger::shred::merkle::make_shreds_from_data(
            &thread_pool,
            &leader_keypair,
            Some(Hash::new_from_array([3u8; 32])),
            &bytes1,
            slot,
            slot - 1,
            0,
            0,
            false,
            0,
            0,
            &reed_solomon_cache,
            &mut ProcessShredsStats::default(),
        )
        .unwrap();
        let data1 = shreds1
            .iter()
            .filter(|s| matches!(s, Shred::ShredData(_)))
            .cloned()
            .collect_vec();
        assert!(!data1.is_empty());
        let end1 = data1.iter().map(|s| s.index()).max().unwrap();

        let entries2 = solana_entry::entry::create_ticks(8, 1, entries1.last().unwrap().hash);
        let bytes2 = bincode::serialize(&entries2).unwrap();
        let shreds2 = solana_ledger::shred::merkle::make_shreds_from_data(
            &thread_pool,
            &leader_keypair,
            Some(Hash::new_from_array([4u8; 32])),
            &bytes2,
            slot,
            slot - 1,
            0,
            0,
            true,
            end1 + 1,
            end1 + 1,
            &reed_solomon_cache,
            &mut ProcessShredsStats::default(),
        )
        .unwrap();
        let data2 = shreds2
            .iter()
            .filter(|s| matches!(s, Shred::ShredData(_)))
            .cloned()
            .collect_vec();
        let coding2 = shreds2
            .iter()
            .filter(|s| matches!(s, Shred::ShredCode(_)))
            .cloned()
            .collect_vec();
        assert!(!data2.is_empty());
        assert!(!coding2.is_empty());
        let start2 = data2
            .iter()
            .map(|s| s.index() as super::ShredIndex)
            .min()
            .unwrap();
        let end2 = data2
            .iter()
            .map(|s| s.index() as super::ShredIndex)
            .max()
            .unwrap();

        // Call 1: omit set-1 terminal boundary to force unknown-start, and poison set-2 start so
        // speculative decode fails.
        let mut packets_call1 = Vec::new();
        for s in data1.iter().filter(|s| s.index() != end1) {
            packets_call1.push(packet_from_payload(s.payload()));
        }
        for s in &data2 {
            let mut payload = s.payload().to_vec();
            if s.index() as super::ShredIndex == start2 {
                payload[88..96].fill(0xFF);
            }
            packets_call1.push(packet_from_payload(&payload));
        }

        let rs_cache = ReedSolomonCache::default();
        let metrics = Arc::new(ShredMetrics::default());
        let mut all_shreds = ahash::HashMap::default();
        let mut slot_fec_keys_to_iterate: Vec<(Slot, super::FecSetKey)> = Vec::new();
        let mut deshredded_entries = Vec::new();
        let mut highest_slot_seen = 0;
        let mut scratch = super::ReconstructScratch::default();
        super::reconstruct_shreds(
            vec![PacketBatch::new(packets_call1)],
            &mut all_shreds,
            &mut slot_fec_keys_to_iterate,
            &mut deshredded_entries,
            &mut highest_slot_seen,
            &rs_cache,
            TEST_RECONSTRUCT_CFG_DECODE_ENTRIES,
            &metrics,
            &mut scratch,
        );
        assert!(
            deshredded_entries.is_empty(),
            "poisoned unknown-start candidate should fail decode"
        );
        let attempts_after_call_1 = metrics.unknown_start_position_count.load(Ordering::Relaxed);
        let errors_after_call_1 = metrics
            .unknown_start_position_error_count
            .load(Ordering::Relaxed);
        assert!(attempts_after_call_1 > 0);
        assert!(errors_after_call_1 > 0);

        // Call 2: add only coding for the same slot/FEC. Data range [start2, end2] is unchanged,
        // so unknown-start speculative decode should be skipped.
        let mut scratch = super::ReconstructScratch::default();
        super::reconstruct_shreds(
            vec![PacketBatch::new(vec![packet_from_payload(
                coding2[0].payload(),
            )])],
            &mut all_shreds,
            &mut slot_fec_keys_to_iterate,
            &mut deshredded_entries,
            &mut highest_slot_seen,
            &rs_cache,
            TEST_RECONSTRUCT_CFG_DECODE_ENTRIES,
            &metrics,
            &mut scratch,
        );
        assert!(
            deshredded_entries.is_empty(),
            "coding-only update should not re-run unchanged unknown-start range"
        );
        assert_eq!(
            metrics.unknown_start_position_count.load(Ordering::Relaxed),
            attempts_after_call_1,
            "unknown-start candidate attempts should not increase without in-range data changes"
        );
        assert_eq!(
            metrics
                .unknown_start_position_error_count
                .load(Ordering::Relaxed),
            errors_after_call_1,
            "unknown-start candidate errors should not increase without in-range data changes"
        );

        let tracker = &all_shreds.get(&slot).unwrap().1;
        assert!(
            !tracker.unknown_start_emitted.contains_key(&end2),
            "failed unknown-start candidate must not be marked as emitted"
        );
    }

    // -----------------------------------------------------------------------------
    // Spec conformance tests for the minute-detail deshred behavior described in `shred_spec.md`.
    // -----------------------------------------------------------------------------

    // These target `solana_ledger::shred::Shredder::deshred` directly so failures are easy to
    // interpret.

    type SpecPayloadMutator = fn(&[Vec<u8>], &[Vec<u8>]) -> Vec<Vec<u8>>;
    type SpecErrorMatcher = fn(&solana_ledger::shred::Error) -> bool;

    struct SpecPayloadFixture {
        expected_entry_count: usize,
        expected_entry_bytes: Vec<u8>,
        data_payloads: Vec<Vec<u8>>,
        coding_payloads: Vec<Vec<u8>>,
    }

    #[derive(Clone, Copy)]
    struct SpecMutationCase {
        name: &'static str,
        slot: Slot,
        entry_count: u64,
        mutate: SpecPayloadMutator,
        error_matches: SpecErrorMatcher,
    }

    fn build_spec_payload_fixture(slot: Slot, entry_count: u64) -> SpecPayloadFixture {
        let leader_keypair = Arc::new(Keypair::new());
        let reed_solomon_cache = ReedSolomonCache::default();
        let shredder = Shredder::new(slot, slot - 1, 0, 0).unwrap();

        let entries = make_slot_entries_with_transactions(entry_count);
        let (data_shreds, coding_shreds) = shredder.entries_to_shreds(
            &leader_keypair,
            entries.as_slice(),
            false,
            Some(Hash::new_from_array([1u8; 32])), // chained_merkle_root
            0,                                     // next_shred_index
            0,                                     // next_code_index
            true,                                  // merkle_variant
            &reed_solomon_cache,
            &mut ProcessShredsStats::default(),
        );
        assert!(!data_shreds.is_empty());
        assert!(!coding_shreds.is_empty());

        let data_payloads = data_shreds
            .iter()
            .sorted_by_key(|s| s.index())
            .map(|s| s.payload().to_vec())
            .collect_vec();
        let coding_payloads = coding_shreds
            .iter()
            .map(|s| s.payload().to_vec())
            .collect_vec();
        SpecPayloadFixture {
            expected_entry_count: entries.len(),
            expected_entry_bytes: bincode::serialize(&entries).unwrap(),
            data_payloads,
            coding_payloads,
        }
    }

    fn spec_mut_missing_index(
        data_payloads: &[Vec<u8>],
        _coding_payloads: &[Vec<u8>],
    ) -> Vec<Vec<u8>> {
        let mut payloads = data_payloads.to_vec();
        assert!(payloads.len() >= 3);
        payloads.remove(payloads.len() / 2);
        payloads
    }

    fn spec_mut_non_consecutive(
        data_payloads: &[Vec<u8>],
        _coding_payloads: &[Vec<u8>],
    ) -> Vec<Vec<u8>> {
        let mut payloads = data_payloads.to_vec();
        assert!(payloads.len() >= 3);
        payloads.swap(0, 1);
        payloads
    }

    fn spec_mut_trailing_after_completion(
        data_payloads: &[Vec<u8>],
        _coding_payloads: &[Vec<u8>],
    ) -> Vec<Vec<u8>> {
        let mut payloads = data_payloads.to_vec();
        payloads.push(payloads[0].clone());
        payloads
    }

    fn spec_mut_invalid_size_low(
        data_payloads: &[Vec<u8>],
        _coding_payloads: &[Vec<u8>],
    ) -> Vec<Vec<u8>> {
        let mut payload = data_payloads[0].clone();
        payload[86..88].copy_from_slice(&0u16.to_le_bytes());
        vec![payload]
    }

    fn spec_mut_invalid_size_high(
        data_payloads: &[Vec<u8>],
        _coding_payloads: &[Vec<u8>],
    ) -> Vec<Vec<u8>> {
        let mut payload = data_payloads[0].clone();
        payload[86..88].copy_from_slice(&u16::MAX.to_le_bytes());
        vec![payload]
    }

    fn spec_mut_invalid_variant_tag(
        data_payloads: &[Vec<u8>],
        _coding_payloads: &[Vec<u8>],
    ) -> Vec<Vec<u8>> {
        let mut payload = data_payloads[0].clone();
        payload[64] = (payload[64] & 0x0F) | 0x50;
        vec![payload]
    }

    fn spec_mut_last_shred_not_complete(
        data_payloads: &[Vec<u8>],
        _coding_payloads: &[Vec<u8>],
    ) -> Vec<Vec<u8>> {
        let mut payloads = data_payloads.to_vec();
        assert!(payloads.len() >= 2);
        let last = payloads.last_mut().unwrap();
        last[85] &= 0x3F;
        payloads
    }

    fn spec_mut_invalid_last_flag_without_data_complete(
        data_payloads: &[Vec<u8>],
        _coding_payloads: &[Vec<u8>],
    ) -> Vec<Vec<u8>> {
        let mut payloads = data_payloads.to_vec();
        assert!(payloads.len() >= 2);
        let last = payloads.last_mut().unwrap();
        // Invalid per spec: LAST_SHRED_IN_SLOT bit set while DATA_COMPLETE_SHRED bit unset.
        last[85] = (last[85] & 0x3F) | 0x80;
        payloads
    }

    fn spec_mut_non_data_shred(
        _data_payloads: &[Vec<u8>],
        coding_payloads: &[Vec<u8>],
    ) -> Vec<Vec<u8>> {
        vec![coding_payloads[0].clone()]
    }

    fn spec_err_is_erasure(err: &solana_ledger::shred::Error) -> bool {
        matches!(err, solana_ledger::shred::Error::ErasureError(_))
    }

    fn spec_err_is_invalid_set(err: &solana_ledger::shred::Error) -> bool {
        matches!(err, solana_ledger::shred::Error::InvalidDeshredSet)
    }

    fn spec_err_is_invalid_data_size(err: &solana_ledger::shred::Error) -> bool {
        matches!(err, solana_ledger::shred::Error::InvalidDataSize { .. })
    }

    fn spec_err_is_invalid_variant(err: &solana_ledger::shred::Error) -> bool {
        matches!(
            err,
            solana_ledger::shred::Error::InvalidShredVariant
                | solana_ledger::shred::Error::InvalidShredType
        )
    }

    fn spec_err_is_invalid_flags(err: &solana_ledger::shred::Error) -> bool {
        matches!(
            err,
            solana_ledger::shred::Error::InvalidShredFlags(_)
                | solana_ledger::shred::Error::ErasureError(_)
        )
    }

    fn spec_err_is_non_data_validation(err: &solana_ledger::shred::Error) -> bool {
        !matches!(err, solana_ledger::shred::Error::InvalidDeshredSet)
    }

    #[test]
    fn test_spec_happy_path_roundtrip() {
        let fixture = build_spec_payload_fixture(111_000, 16);

        let deshredded_payload =
            Shredder::deshred(fixture.data_payloads.iter().map(Vec::as_slice)).unwrap();
        // Deshred output should match the serialized `Vec<Entry>` bytes.
        assert_eq!(deshredded_payload, fixture.expected_entry_bytes);

        // And the bytes should be decodable into our local Entry type with trailing tolerance.
        let decoded = deserialize_entries_allow_trailing(&deshredded_payload);
        assert_eq!(decoded.len(), fixture.expected_entry_count);
    }

    #[test]
    fn test_spec_mutation_cases_fail_as_expected() {
        const CASES: [SpecMutationCase; 9] = [
            SpecMutationCase {
                name: "missing_index",
                slot: 111_001,
                entry_count: 64,
                mutate: spec_mut_missing_index,
                error_matches: spec_err_is_erasure,
            },
            SpecMutationCase {
                name: "non_consecutive_indices",
                slot: 111_002,
                entry_count: 64,
                mutate: spec_mut_non_consecutive,
                error_matches: spec_err_is_erasure,
            },
            SpecMutationCase {
                name: "trailing_after_completion",
                slot: 111_003,
                entry_count: 16,
                mutate: spec_mut_trailing_after_completion,
                error_matches: spec_err_is_invalid_set,
            },
            SpecMutationCase {
                name: "invalid_size_low",
                slot: 111_004,
                entry_count: 16,
                mutate: spec_mut_invalid_size_low,
                error_matches: spec_err_is_invalid_data_size,
            },
            SpecMutationCase {
                name: "invalid_size_high",
                slot: 111_004_001,
                entry_count: 16,
                mutate: spec_mut_invalid_size_high,
                error_matches: spec_err_is_invalid_data_size,
            },
            SpecMutationCase {
                name: "invalid_variant_tag",
                slot: 111_004_002,
                entry_count: 16,
                mutate: spec_mut_invalid_variant_tag,
                error_matches: spec_err_is_invalid_variant,
            },
            SpecMutationCase {
                name: "last_shred_without_data_complete",
                slot: 111_005,
                entry_count: 64,
                mutate: spec_mut_last_shred_not_complete,
                error_matches: spec_err_is_erasure,
            },
            SpecMutationCase {
                name: "invalid_last_flag_without_data_complete",
                slot: 111_005_100,
                entry_count: 64,
                mutate: spec_mut_invalid_last_flag_without_data_complete,
                error_matches: spec_err_is_invalid_flags,
            },
            SpecMutationCase {
                name: "reject_non_data_shred",
                slot: 111_006,
                entry_count: 16,
                mutate: spec_mut_non_data_shred,
                error_matches: spec_err_is_non_data_validation,
            },
        ];

        for case in CASES {
            let fixture = build_spec_payload_fixture(case.slot, case.entry_count);
            let payloads = (case.mutate)(&fixture.data_payloads, &fixture.coding_payloads);
            let err = Shredder::deshred(payloads.iter().map(Vec::as_slice)).unwrap_err();
            assert!(
                (case.error_matches)(&err),
                "spec case '{}' expected different error; got: {err:?}",
                case.name
            );
        }
    }

    #[test]
    fn test_spec_empty_output_behavior_deserializes_empty_entries() {
        let slot = 111_007;
        let leader_keypair = Arc::new(Keypair::new());
        let reed_solomon_cache = ReedSolomonCache::default();
        let shredder = Shredder::new(slot, slot - 1, 0, 0).unwrap();

        let entries = make_slot_entries_with_transactions(16);
        let (data_shreds, _coding_shreds) = shredder.entries_to_shreds(
            &leader_keypair,
            entries.as_slice(),
            false,
            Some(Hash::new_from_array([1u8; 32])), // chained_merkle_root
            0,                                     // next_shred_index
            0,                                     // next_code_index
            true,                                  // merkle_variant
            &reed_solomon_cache,
            &mut ProcessShredsStats::default(),
        );
        assert!(!data_shreds.is_empty());

        // Force the shred to contribute 0 bytes by setting size == 88 (SIZE_OF_DATA_HEADERS).
        // With DATA_COMPLETE_SHRED set, this exercises the spec's backward-compat empty-output
        // behavior (deshred returns a non-empty zero-filled buffer).
        let mut payload = data_shreds
            .iter()
            .max_by_key(|s| s.index())
            .unwrap()
            .payload()
            .to_vec();
        payload[86..88].copy_from_slice(&(88u16).to_le_bytes());

        let deshredded_payload = Shredder::deshred(std::iter::once(payload.as_slice())).unwrap();
        assert!(
            !deshredded_payload.is_empty(),
            "expected non-empty zero-filled buffer"
        );
        let expected_fallback_len =
            solana_ledger::shred::ShredData::capacity(/*merkle_proof_size:*/ None).unwrap();
        assert_eq!(
            deshredded_payload.len(),
            expected_fallback_len,
            "empty-output fallback should track upstream Shredder::deshred behavior"
        );
        assert!(
            deshredded_payload.iter().all(|&b| b == 0),
            "expected empty-output fallback buffer to be all zeros"
        );

        let decoded = deserialize_entries_allow_trailing(&deshredded_payload);
        assert!(
            decoded.is_empty(),
            "expected deserialization to yield an empty Vec<Entry>"
        );
    }
}
