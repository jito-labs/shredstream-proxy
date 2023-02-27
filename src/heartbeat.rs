use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::{sleep, Builder, JoinHandle},
    time::Duration,
};

use crossbeam_channel::Receiver;
use jito_protos::{
    auth::{auth_service_client::AuthServiceClient, Role},
    shredstream::{shredstream_client::ShredstreamClient, Heartbeat},
};
use log::{info, warn};
use solana_metrics::{datapoint_info, datapoint_warn};
use solana_sdk::signature::Keypair;
use tokio::runtime::Runtime;
use tonic::{codegen::InterceptedService, transport::Channel, Code};

use crate::{
    token_authenticator::{create_grpc_channel, ClientInterceptor},
    ShredstreamProxyError,
};

#[derive(Debug, Clone)]
pub struct LogContext {
    pub solana_cluster: String,
    pub region: String,
}

#[allow(clippy::too_many_arguments)]
pub fn heartbeat_loop_thread(
    block_engine_url: String,
    auth_keypair: &Arc<Keypair>,
    desired_regions: Vec<String>,
    recv_socket: SocketAddr,
    log_context: Option<LogContext>,
    runtime: Runtime,
    shutdown_receiver: Receiver<()>,
    exit: Arc<AtomicBool>,
) -> JoinHandle<()> {
    let auth_keypair = auth_keypair.clone();
    Builder::new().name("shredstream_proxy-heartbeat_loop_thread".to_string()).spawn(move || {
        let heartbeat_socket = jito_protos::shared::Socket {
            ip: recv_socket.ip().to_string(),
            port: recv_socket.port() as i64,
        };
        let (mut heartbeat_interval, mut failed_heartbeat_interval) = (Duration::from_secs(1), Duration::from_secs(1)); //start with 1s, change based on server suggestion
        // use tick() since we want to avoid thread::sleep(), as it's not interruptable. want to be interruptable for exiting quickly
        let mut heartbeat_tick = crossbeam_channel::tick(heartbeat_interval);
        let metrics_tick = crossbeam_channel::tick(Duration::from_secs(30));
        let mut client_restart_count = 0u64;
        let mut successful_heartbeat_count = 0u64;
        let mut failed_heartbeat_count = 0u64;

        while !exit.load(Ordering::Relaxed) {
            let shredstream_client = runtime.block_on(get_grpc_client(&block_engine_url, &auth_keypair, exit.clone()));

            let mut shredstream_client = match shredstream_client {
                Ok(c) => c,
                Err(e) => {
                    warn!("Failed to connect to block engine, retrying. Error: {e}");
                    client_restart_count += 1;
                    if let Some(log_ctx) = &log_context {
                        datapoint_warn!("shredstream_proxy-heartbeat_client_error",
                                            "solana_cluster" => log_ctx.solana_cluster,
                                            "region" => log_ctx.region,
                                            "block_engine_url" => block_engine_url,
                                            ("errors", 1, i64),
                                            ("error_str", e.to_string(), String),
                            )
                    }
                    if !exit.load(Ordering::Relaxed) {
                        sleep(failed_heartbeat_interval);
                    }
                    continue; // avoid sending heartbeat, try acquiring grpc client again
                }
            };

            while !exit.load(Ordering::Relaxed) {
                crossbeam_channel::select! {
                    recv(heartbeat_tick) -> _ => {
                        let heartbeat_result = runtime.block_on(shredstream_client
                            .send_heartbeat(Heartbeat {
                                socket: Some(heartbeat_socket.clone()),
                                regions: desired_regions.clone(),
                            }));

                        match heartbeat_result {
                            Ok(x) => {
                                let new_ttl = x.get_ref().ttl_ms as u64;
                                let new_interval = Duration::from_millis(new_ttl.checked_div(2).unwrap());
                                if heartbeat_interval != new_interval {
                                    info!("Sending heartbeat every {new_interval:?}.");
                                    heartbeat_interval = new_interval;
                                    heartbeat_tick = crossbeam_channel::tick(new_interval);
                                    failed_heartbeat_interval = Duration::from_millis(new_ttl / 4);
                                }
                                successful_heartbeat_count += 1;
                            }
                            Err(err) => {
                                if err.code() == Code::InvalidArgument {
                                    panic!("Invalid arguments: {err}.");
                                };
                                warn!("Error sending heartbeat: {err}");
                                if let Some(log_ctx) = &log_context {
                                    datapoint_warn!("shredstream_proxy-heartbeat_send_error",
                                                    "solana_cluster" => log_ctx.solana_cluster,
                                                    "region" => log_ctx.region,
                                                    "block_engine_url" => block_engine_url,
                                                    ("errors", 1, i64),
                                                    ("error_str", err.to_string(), String),
                                   );
                                }
                                failed_heartbeat_count += 1;
                            }
                        }
                    }
                    recv(metrics_tick) -> _ => {
                        if let Some(log_ctx) = &log_context {
                            datapoint_info!("shredstream_proxy-heartbeat_stats",
                                            "solana_cluster" => log_ctx.solana_cluster,
                                            "region" => log_ctx.region,
                                            "block_engine_url" => block_engine_url,
                                            ("successful_heartbeat_count", successful_heartbeat_count, i64),
                                            ("failed_heartbeat_count", failed_heartbeat_count, i64),
                                            ("client_restart_count", client_restart_count, i64),
                            );
                        }
                    }
                    recv(shutdown_receiver) -> _ => {
                        break;
                    }
                }
            }
        }
        info!("Exiting heartbeat thread, sent {successful_heartbeat_count} successful, {failed_heartbeat_count} failed heartbeats. Client restarted {client_restart_count} times.");
    }).unwrap()
}

pub async fn get_grpc_client(
    block_engine_url: &str,
    auth_keypair: &Arc<Keypair>,
    exit: Arc<AtomicBool>,
) -> Result<ShredstreamClient<InterceptedService<Channel, ClientInterceptor>>, ShredstreamProxyError>
{
    let auth_channel = create_grpc_channel(block_engine_url).await?;
    let client_interceptor = ClientInterceptor::new(
        AuthServiceClient::new(auth_channel),
        auth_keypair,
        Role::ShredstreamSubscriber,
        exit,
    )
    .await?;

    let searcher_channel = create_grpc_channel(block_engine_url).await?;
    let searcher_client = ShredstreamClient::with_interceptor(searcher_channel, client_interceptor);
    Ok(searcher_client)
}
