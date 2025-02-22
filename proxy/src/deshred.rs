use std::{
    collections::{BTreeSet, HashMap},
    net::SocketAddr,
    sync::{atomic::AtomicBool, Arc, RwLock},
    thread,
    thread::JoinHandle,
};

use crossbeam_channel::Receiver;
use itertools::Itertools;
use jito_protos::deshred::{
    deshred_service_server::{DeshredService, DeshredServiceServer},
    DeshredRequest, Entry as DeshredEntry,
};
use log::*;
use solana_entry::entry::Entry;
use solana_ledger::shred::{self, Shred, Shredder};
use solana_packet::Packet;
use solana_sdk::clock::Slot;
use tokio::sync::{
    broadcast::{Receiver as BroadcastReceiver, Sender},
    mpsc,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Server, Status};

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

pub fn insert_packet_into_shred_bucket(
    packet: &Packet,
    shred_bucket_by_slot: &Arc<RwLock<HashMap<Slot, BTreeSet<WrappedShred>>>>,
    entry_sender: &Arc<Sender<DeshredEntry>>,
) {
    let shred = packet_to_shred(packet);
    if shred.is_none() {
        return;
    }
    let shred = shred.unwrap();
    let slot = shred.slot();

    if !shred.is_data() {
        return;
    }

    // debug!("shred slot {:?} idx {:?}", slot, shred.index());
    // create a new slot bucket if it doesn't exist, else insert the shred into the existing slot bucket
    let mut shred_bucket_by_slot = shred_bucket_by_slot.write().unwrap();
    let slot_bucket = shred_bucket_by_slot.get(&slot);
    if slot_bucket.is_none() {
        shred_bucket_by_slot.insert(slot, BTreeSet::new());
    }
    let slot_bucket = shred_bucket_by_slot.get_mut(&slot).unwrap();
    slot_bucket.insert(WrappedShred(shred));

    let last_shred = slot_bucket.last().unwrap();
    if last_shred.0.last_in_slot() && slot_bucket.len() == (last_shred.0.index() + 1) as usize {
        debug!("deshredding slot {:?}", slot);
        let shreds = slot_bucket
            .iter()
            .map(|shred| shred.0.clone())
            .collect_vec();

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
            if entry_sender.receiver_count() > 0 {
                for entry in entries {
                    let send_result = entry_sender.send(DeshredEntry {
                        num_hashes: entry.num_hashes,
                        hash: entry.hash.to_bytes().to_vec(),
                        // tbd: is bincode supported in other languages ?
                        transactions: entry
                            .transactions
                            .iter()
                            .map(|t| bincode::serialize(t).unwrap())
                            .collect_vec(),
                    });
                    if send_result.is_err() {
                        error!(
                            "failed to send deshredded entry to deshred server {:?}",
                            send_result.err()
                        );
                    }
                }
            }
            shred_bucket_by_slot.remove(&slot);
        } else {
            debug!("failed to deshred entries {:?}", entries.err());
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
        Err(_) => None,
    }
}

#[derive(Debug)]
pub struct DeshredServer {
    entry_sender: Arc<Sender<DeshredEntry>>,
}

#[tonic::async_trait]
impl DeshredService for DeshredServer {
    type SubscribeToEntriesStream = ReceiverStream<Result<DeshredEntry, Status>>;

    async fn subscribe_to_entries(
        &self,
        _request: tonic::Request<DeshredRequest>,
    ) -> Result<tonic::Response<Self::SubscribeToEntriesStream>, Status> {
        let (tx, rx) = mpsc::channel(100);
        let mut entry_receiver: BroadcastReceiver<DeshredEntry> = self.entry_sender.subscribe();

        tokio::spawn(async move {
            while let Ok(entry) = entry_receiver.recv().await {
                match tx.send(Ok(entry)).await {
                    Ok(_) => (),
                    Err(e) => {
                        debug!("client disconnected");
                        break;
                    }
                }
            }
        });

        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }
}

pub fn start_deshred_server_thread(
    addr: SocketAddr,
    entry_sender: Arc<Sender<DeshredEntry>>,
    exit: Arc<AtomicBool>,
    shutdown_receiver: Receiver<()>,
) -> JoinHandle<()> {
    thread::spawn(move || {
        let entry_sender = entry_sender.clone();
        let deshred_server = DeshredServer {
            entry_sender: entry_sender,
        };

        let runtime = tokio::runtime::Runtime::new().unwrap();

        let server_handle = runtime.spawn(async move {
            info!("starting deshred server on {:?}", addr);
            Server::builder()
                .add_service(DeshredServiceServer::new(deshred_server))
                .serve(addr)
                .await
                .unwrap();
        });

        while !exit.load(std::sync::atomic::Ordering::Relaxed) {
            if let Ok(_) = shutdown_receiver.recv() {
                server_handle.abort();
                info!("shutting down deshred server");
                break;
            }
        }
    })
}
