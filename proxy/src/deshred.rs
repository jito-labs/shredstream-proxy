use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    sync::atomic::Ordering,
};

use itertools::Itertools;
use jito_protos::shredstream::TraceShred;
use log::{debug, warn};
use prost::Message;
use solana_ledger::shred::{
    merkle::{Shred, ShredCode},
    ReedSolomonCache, Shredder,
};
use solana_sdk::clock::{Slot, MAX_PROCESSING_AGE};

use crate::forwarder::ShredMetrics;

/// Returns the number of shreds reconstructed
/// Updates all_shreds with current state, and deshredded_entries with returned values
pub fn reconstruct_shreds<'a, I: Iterator<Item = &'a [u8]>>(
    packet_batch_vec: I,
    all_shreds: &mut HashMap<
        Slot,
        HashMap<u32 /* fec_set_index */, (bool /* completed */, HashSet<ComparableShred>)>,
    >,
    deshredded_entries: &mut Vec<(Slot, Vec<solana_entry::entry::Entry>, Vec<u8>)>,
    rs_cache: &ReedSolomonCache,
    metrics: &ShredMetrics,
) -> usize {
    deshredded_entries.clear();
    let mut slot_fec_index_to_iterate = HashSet::new();
    for data in packet_batch_vec {
        match solana_ledger::shred::Shred::new_from_serialized_shred(data.to_vec())
            .and_then(Shred::try_from)
        {
            Ok(shred) => {
                let slot = shred.common_header().slot;
                let fec_set_index = shred.fec_set_index();
                all_shreds
                    .entry(slot)
                    .or_default()
                    .entry(fec_set_index)
                    .or_default()
                    .1
                    .insert(ComparableShred(shred));
                slot_fec_index_to_iterate.insert((slot, fec_set_index));
            }
            Err(e) => {
                if TraceShred::decode(data).is_ok() {
                    continue;
                }
                warn!("Failed to decode shred. Err: {e:?}");
            }
        }
    }

    let mut recovered_count = 0;
    let mut highest_slot_seen = 0;
    for (slot, fec_set_index) in slot_fec_index_to_iterate {
        highest_slot_seen = highest_slot_seen.max(slot);
        let Some((already_deshredded, shreds)) = all_shreds
            .get_mut(&slot)
            .and_then(|fec_set_indexes| fec_set_indexes.get_mut(&fec_set_index))
        else {
            continue;
        };
        if *already_deshredded {
            debug!("already completed slot {slot}");
            continue;
        }

        let (num_expected_data_shreds, num_data_shreds) = can_recover(shreds);

        // haven't received last data shred, haven't seen any coding shreds, so wait until more arrive
        if num_expected_data_shreds == 0
            || (num_data_shreds < num_expected_data_shreds
                && shreds.len() < num_data_shreds as usize)
        {
            // not enough data shreds, not enough shreds to recover
            continue;
        }

        // try to recover if we have enough coding shreds
        let mut recovered_shreds = Vec::new();
        if num_data_shreds < num_expected_data_shreds
            && shreds.len() as u16 >= num_expected_data_shreds
        {
            let merkle_shreds = shreds
                .iter()
                .sorted_by_key(|s| (u8::MAX - s.shred_type() as u8, s.index()))
                .map(|s| s.0.clone())
                .collect_vec();
            let recovered = match solana_ledger::shred::merkle::recover(merkle_shreds, rs_cache) {
                Ok(r) => r,
                Err(e) => {
                    warn!("Failed to recover shreds for slot: {slot}, fec set: {fec_set_index}. Err: {e}");
                    continue;
                }
            };

            for shred in recovered {
                match shred {
                    Ok(shred) => {
                        recovered_count += 1;
                        recovered_shreds.push(ComparableShred(shred));
                        // can also just insert into hashmap, but kept separate for ease of debug
                    }
                    Err(e) => warn!(
                        "Failed to recover shred for slot {slot}, fec set: {fec_set_index}. Err: {e}"
                    ),
                }
            }
        }

        let sorted_deduped_data_payloads = shreds
            .iter()
            .chain(recovered_shreds.iter())
            .filter_map(|s| Some((s, solana_ledger::shred::layout::get_data(s.payload()).ok()?)))
            .sorted_by_key(|(s, _data)| s.index())
            .collect_vec();

        if (sorted_deduped_data_payloads.len() as u16) < num_expected_data_shreds {
            continue;
        }

        let deshred_payload = match Shredder::deshred_unchecked(
            sorted_deduped_data_payloads
                .iter()
                .map(|(s, _data)| s.payload()),
        ) {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    "slot {slot} failed to deshred fec_set_index {fec_set_index}. num_expected_data_shreds: {num_expected_data_shreds}, num_data_shreds: {num_data_shreds}. shred set len: {}, recovered shred set len: {},  Err: {e}.",
                    shreds.len(),
                    recovered_shreds.len(),
                );
                metrics
                    .fec_recovery_error_count
                    .fetch_add(1, Ordering::Relaxed);
                continue;
            }
        };
        let entries = match bincode::deserialize::<Vec<solana_entry::entry::Entry>>(
            &deshred_payload,
        ) {
            Ok(e) => e,
            Err(e) => {
                warn!(
                        "slot {slot} fec_set_index {fec_set_index} failed to deserialize bincode payload of size {}. Err: {e}",
                        deshred_payload.len()
                    );
                metrics
                    .bincode_deserialize_error_count
                    .fetch_add(1, Ordering::Relaxed);
                continue;
            }
        };
        metrics
            .entry_count
            .fetch_add(entries.len() as u64, Ordering::Relaxed);
        let txn_count = entries.iter().map(|e| e.transactions.len() as u64).sum();
        metrics.txn_count.fetch_add(txn_count, Ordering::Relaxed);
        debug!(
            "Successfully decoded slot: {slot} fec_index: {fec_set_index} with entry count: {}, txn count: {txn_count}",
            entries.len(),
        );

        deshredded_entries.push((slot, entries, deshred_payload));
        if let Some(fec_set) = all_shreds.get_mut(&slot) {
            // done with this fec set index
            let _ = fec_set
                .get_mut(&fec_set_index)
                .map(|(is_completed, fec_set_shreds)| {
                    *is_completed = true;
                    fec_set_shreds.clear();
                });
        }
    }

    if all_shreds.len() > MAX_PROCESSING_AGE && highest_slot_seen > SLOT_LOOKBACK {
        let threshold = highest_slot_seen - SLOT_LOOKBACK;
        all_shreds.retain(|slot, _fec_set_index| *slot >= threshold);
    }

    if recovered_count > 0 {
        metrics
            .recovered_count
            .fetch_add(recovered_count as u64, Ordering::Relaxed);
    }

    recovered_count
}

const SLOT_LOOKBACK: Slot = 50;

/// check if we can reconstruct (having minimum number of data + coding shreds)
fn can_recover(
    shreds: &HashSet<ComparableShred>,
) -> (
    u16, /* num_expected_data_shreds */
    u16, /* num_data_shreds */
) {
    let mut num_expected_data_shreds = 0;
    let mut data_shred_count = 0;
    for shred in shreds {
        match &shred.0 {
            Shred::ShredCode(s) => {
                num_expected_data_shreds = s.coding_header.num_data_shreds;
            }
            Shred::ShredData(s) => {
                data_shred_count += 1;
                if num_expected_data_shreds == 0 && (s.data_complete() || s.last_in_slot()) {
                    num_expected_data_shreds =
                        (shred.0.index() - shred.0.fec_set_index()) as u16 + 1;
                }
            }
        }
    }
    (num_expected_data_shreds, data_shred_count)
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
        collections::{hash_map, HashMap, HashSet},
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
    use solana_perf::packet::Packet;
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

        let mut map = HashMap::<usize, usize>::new();
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
                        hash_map::Entry::Occupied(mut e) => {
                            *e.get_mut() += 1;
                        }
                        hash_map::Entry::Vacant(e) => {
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
        let mut deshredded_entries = Vec::new();
        let mut all_shreds: HashMap<
            Slot,
            HashMap<u32 /* fec_set_index */, (bool /* completed */, HashSet<ComparableShred>)>,
        > = HashMap::new();
        let recovered_count = reconstruct_shreds(
            shreds.iter().map(|shred| shred.payload().iter().as_slice()),
            &mut all_shreds,
            &mut deshredded_entries,
            &rs_cache,
            &metrics,
        );
        assert!(recovered_count < deshredded_entries.len());
        assert_eq!(
            deshredded_entries
                .iter()
                .map(|(_slot, entries, _entries_bytes)| entries.len())
                .sum::<usize>(),
            13580
        );
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
        assert_eq!(slot_to_entry.len(), 29);

        // Test 2: 33% of shreds missing
        let mut deshredded_entries = Vec::new();
        let mut all_shreds: HashMap<
            Slot,
            HashMap<u32 /* fec_set_index */, (bool /* completed */, HashSet<ComparableShred>)>,
        > = HashMap::new();
        let recovered_count = reconstruct_shreds(
            shreds
                .iter()
                .enumerate()
                .filter(|(index, _)| (index + 1) % 3 != 0)
                .map(|(_, shred)| shred.payload().iter().as_slice()),
            &mut all_shreds,
            &mut deshredded_entries,
            &rs_cache,
            &metrics,
        );

        assert!(recovered_count > (deshredded_entries.len() / 4));
        assert_eq!(
            deshredded_entries
                .iter()
                .map(|(_slot, entries, _entries_bytes)| entries.len())
                .sum::<usize>(),
            13580
        );
        assert!(all_shreds.len() > 15);

        let slot_to_entry = deshredded_entries
            .iter()
            .into_group_map_by(|(slot, _entries, _entries_bytes)| *slot);
        assert_eq!(slot_to_entry.len(), 29);
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
        let mut deshredded_entries = Vec::new();
        let mut all_shreds: HashMap<
            Slot,
            HashMap<u32 /* fec_set_index */, (bool /* completed */, HashSet<ComparableShred>)>,
        > = HashMap::new();
        let recovered_count = reconstruct_shreds(
            packets.iter().map(|s| s.data(..).unwrap()),
            &mut all_shreds,
            &mut deshredded_entries,
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
        let mut deshredded_entries = Vec::new();
        let mut all_shreds: HashMap<
            Slot,
            HashMap<u32 /* fec_set_index */, (bool /* completed */, HashSet<ComparableShred>)>,
        > = HashMap::new();
        let recovered_count = reconstruct_shreds(
            packets
                .iter()
                .enumerate()
                .filter(|(index, _)| (index + 1) % 3 != 0)
                .map(|(_, s)| s.data(..).unwrap()),
            &mut all_shreds,
            &mut deshredded_entries,
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
