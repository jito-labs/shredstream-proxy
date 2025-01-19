
use itertools::Itertools;
use solana_ledger::shred::{self, Shred, Shredder};

use solana_packet::Packet;
use solana_sdk::clock::Slot;
use solana_entry::entry::Entry;

use std::collections::HashMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::RwLock;
use log::*;


pub struct WrappedShred(Shred);

impl Ord for WrappedShred {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.index().cmp(&other.0.index())
    }
}

impl PartialOrd for WrappedShred {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.index().partial_cmp(&other.0.index())
    }
}

impl PartialEq for WrappedShred {
    fn eq(&self, other: &Self) -> bool {
        self.0.index() == other.0.index()
    }
}

impl Eq for WrappedShred {}


pub fn insert_packet(packet: &Packet, shred_bucket_by_slot:  &Arc<RwLock<HashMap<Slot, BTreeSet<WrappedShred>>>>) {
    let shred = packet_to_shred(packet);
    if shred.is_none() {
        return;
    }
    let shred = shred.unwrap(); 
    let slot = shred.slot();
    if shred.is_data(){ 
        // debug!("shred slot {:?} idx {:?}", slot, shred.index());
        // create a new slot bucket if it doesn't exist, else insert the shred into the existing slot bucket
        let mut shred_bucket_by_slot = shred_bucket_by_slot.write().unwrap();
        let slot_bucket  = shred_bucket_by_slot.get(&slot);
        if slot_bucket.is_none() {
            shred_bucket_by_slot.insert(slot, BTreeSet::new());
        }
        let slot_bucket = shred_bucket_by_slot.get_mut(&slot).unwrap();
        slot_bucket.insert(WrappedShred(shred));


        let last_shred = slot_bucket.last().unwrap(); 
        if last_shred.0.last_in_slot()  && slot_bucket.len() == (last_shred.0.index() + 1) as usize{ 
            debug!("deshredding slot {:?}", slot);
            let shreds = slot_bucket.iter().map(|shred| shred.0.clone()).collect_vec(); 
            
            let data = Shredder::deshred(shreds.as_slice());
            if data.is_err() {
                debug!("failed to deshred shreds {:?}", data.err());
                return;
            }
            let data = data.unwrap();
            let entries: Result<Vec<Entry>, _> = bincode::deserialize(&data);
            if entries.is_ok() {
                let entries = entries.unwrap();
                debug!("deshredded entries {:?}", entries);
            } else {
                debug!("failed to deshred entries {:?}", entries.err());
            } 
        }

    }
}

fn packet_to_shred(packet: &Packet) -> Option<Shred> {
    let shred = shred::layout::get_shred(packet);

    if shred.is_none() {
        return None;
    }

    match Shred::new_from_serialized_shred(shred.unwrap().to_vec()) {
        Ok(shred) => Some(shred),
        Err(_) => None
    }
}