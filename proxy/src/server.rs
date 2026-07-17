use std::{
    net::SocketAddr,
    sync::{atomic::AtomicBool, Arc},
    thread::JoinHandle,
    time::Duration,
};

use crossbeam_channel::Receiver;
use jito_protos::shredstream::{
    shredstream_proxy_server::{ShredstreamProxy, ShredstreamProxyServer},
    Entry as PbEntry, SubscribeEntriesRequest,
};
use log::{debug, info};
use tokio::sync::broadcast::{Receiver as BroadcastReceiver, Sender};
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;

#[derive(Debug)]
pub struct ShredstreamProxyService {
    entry_sender: Arc<Sender<PbEntry>>,
}

pub fn start_server_thread(
    addr: SocketAddr,
    entry_sender: Arc<Sender<PbEntry>>,
    exit: Arc<AtomicBool>,
    shutdown_receiver: Receiver<()>,
) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Runtime::new().unwrap();

        let server_handle = runtime.spawn(async move {
            info!("starting server on {:?}", addr);
            tonic::transport::Server::builder()
                .add_service(ShredstreamProxyServer::new(ShredstreamProxyService {
                    entry_sender,
                }))
                .serve(addr)
                .await
                .unwrap();
        });

        while !exit.load(std::sync::atomic::Ordering::Relaxed) {
            if shutdown_receiver
                .recv_timeout(Duration::from_secs(1))
                .is_ok()
            {
                server_handle.abort();
                info!("shutting down entries server");
                break;
            }
        }
    })
}
#[tonic::async_trait]
impl ShredstreamProxy for ShredstreamProxyService {
    type SubscribeEntriesStream = ReceiverStream<Result<PbEntry, tonic::Status>>;

    async fn subscribe_entries(
        &self,
        _request: tonic::Request<SubscribeEntriesRequest>,
    ) -> Result<tonic::Response<Self::SubscribeEntriesStream>, tonic::Status> {
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let mut entry_receiver: BroadcastReceiver<PbEntry> = self.entry_sender.subscribe();

        tokio::spawn(async move {
            loop {
                match entry_receiver.recv().await {
                    Ok(entry) => {
                        if tx.send(Ok(entry)).await.is_err() {
                            debug!("client disconnected");
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        // Receiver fell behind; keep streaming the latest entries.
                        continue;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                };
            }
        });

        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }
}
