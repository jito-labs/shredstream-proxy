use std::{
    net::SocketAddr,
    ops::Sub,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::{sleep, Builder, JoinHandle},
    time::{Duration, Instant},
};

use jito_protos::{
    auth::{auth_service_client::AuthServiceClient, Role},
    shredstream::{shredstream_client::ShredstreamClient, Heartbeat},
};
use log::{error, info, warn};
use solana_metrics::datapoint_warn;
use solana_sdk::signature::Keypair;
use tokio::runtime::Runtime;
use tonic::{codegen::InterceptedService, transport::Channel, Code};

use crate::{
    token_authenticator::{create_grpc_channel, ClientInterceptor},
    ShredstreamProxyError,
};

pub fn heartbeat_loop_thread(
    block_engine_url: String,
    auth_keypair: &Arc<Keypair>,
    regions: Vec<String>,
    recv_socket: SocketAddr,
    runtime: Runtime,
    exit: Arc<AtomicBool>,
) -> JoinHandle<()> {
    let auth_keypair = auth_keypair.clone();
    Builder::new()
        .name("shredstream-proxy-heartbeat-loop-thread".to_string())
        .spawn(move || {
            let mut successful_heartbeat_count: u64 = 0;
            let mut failed_heartbeat_count: u64 = 0;
            let mut shredstream_client = runtime
                .block_on(get_grpc_client(&block_engine_url, &auth_keypair))
                .expect("failed to connect to block engine");
            let (mut heartbeat_interval, mut failed_heartbeat_interval) = (Duration::from_millis(100), Duration::from_millis(100)); //start with 100ms, change based on server suggestion
            let heartbeat_socket = jito_protos::shared::Socket {
                ip: recv_socket.ip().to_string(),
                port: recv_socket.port() as i64,
            };

            while !exit.load(Ordering::Relaxed) {
                let start = Instant::now();
                let heartbeat_result = runtime.block_on(shredstream_client
                    .send_heartbeat(Heartbeat {
                        socket: Some(heartbeat_socket.clone()),
                        regions: regions.clone(),
                    }));

                match heartbeat_result {
                    Ok(x) => {
                        let new_ttl = x.get_ref().ttl_ms as u64;
                        let new_interval = Duration::from_millis(new_ttl.checked_div(2).unwrap());
                        if heartbeat_interval != new_interval {
                            info!("Setting heartbeat interval to {new_interval:?}.");
                            heartbeat_interval = new_interval;
                            failed_heartbeat_interval = Duration::from_millis(new_ttl / 4);
                        }
                        successful_heartbeat_count += 1;

                        let elapsed = start.elapsed();
                        if elapsed.lt(&heartbeat_interval) {
                            sleep(heartbeat_interval.sub(elapsed));
                        }
                    }
                    Err(err) => {
                        if err.code() == Code::InvalidArgument {
                            exit.store(true, Ordering::SeqCst);
                            error!("Invalid arguments: {err}");
                            return;
                        };
                        warn!("Error sending heartbeat: {err}");
                        datapoint_warn!("heartbeat_send_error", ("errors", 1, i64));
                        failed_heartbeat_count += 1;

                        // sleep faster to avoid getting deleted via TTL expiration in NATS
                        let elapsed = start.elapsed();
                        if elapsed.lt(&failed_heartbeat_interval) {
                            sleep(failed_heartbeat_interval.sub(elapsed));
                        }
                    }
                }
                let elapsed = start.elapsed();
                if elapsed.lt(&heartbeat_interval) {
                    sleep(heartbeat_interval.sub(elapsed));
                }
            }
            info!("Exiting heartbeat thread, sent {successful_heartbeat_count} successful, {failed_heartbeat_count} failed shreds.");
        })
        .unwrap()
}

pub async fn get_grpc_client(
    block_engine_url: &str,
    auth_keypair: &Arc<Keypair>,
) -> Result<ShredstreamClient<InterceptedService<Channel, ClientInterceptor>>, ShredstreamProxyError>
{
    let auth_channel = create_grpc_channel(block_engine_url).await?;
    let client_interceptor = ClientInterceptor::new(
        AuthServiceClient::new(auth_channel),
        auth_keypair,
        Role::ShredstreamSubscriber,
    )
    .await?;

    let searcher_channel = create_grpc_channel(block_engine_url).await?;
    let searcher_client = ShredstreamClient::with_interceptor(searcher_channel, client_interceptor);
    Ok(searcher_client)
}
