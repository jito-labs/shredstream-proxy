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
use rand::{thread_rng, Rng};
use solana_metrics::{datapoint_info, datapoint_warn};
use solana_sdk::signature::Keypair;
use tokio::runtime::Runtime;
use tonic::{codegen::InterceptedService, transport::Channel, Code};

use crate::{
    token_authenticator::{create_grpc_channel, ClientInterceptor},
    ShredstreamProxyError,
};

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
    heartbeat_stats_sampling_prob: f64,
    runtime: Runtime,
    exit: Arc<AtomicBool>,
) -> JoinHandle<()> {
    let auth_keypair = auth_keypair.clone();
    Builder::new()
        .name("shredstream_proxy-heartbeat_loop_thread".to_string())
        .spawn(move || {
            let mut client_restart_count = 0u64;
            let mut successful_heartbeat_count = 0u64;
            let mut failed_heartbeat_count = 0u64;
            let (mut heartbeat_interval, mut failed_heartbeat_interval) = (Duration::from_millis(100), Duration::from_millis(100)); //start with 100ms, change based on server suggestion
            let heartbeat_socket = jito_protos::shared::Socket {
                ip: recv_socket.ip().to_string(),
                port: recv_socket.port() as i64,
            };
            let mut rng = thread_rng();
            while !exit.load(Ordering::Relaxed) {
                let shredstream_client = runtime
                    .block_on(get_grpc_client(&block_engine_url, &auth_keypair));

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
                        continue;
                    }
                };
                while !exit.load(Ordering::Relaxed) {
                    let start = Instant::now();
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
                                error!("Invalid arguments: {err}.");
                                return;
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

                            // sleep for shorter time period to avoid TTL expiration in NATS
                            let elapsed = start.elapsed();
                            if elapsed.lt(&failed_heartbeat_interval) {
                                sleep(failed_heartbeat_interval.sub(elapsed));
                            }
                        }
                    }

                    if let Some(log_ctx) = &log_context {
                        if rng.gen_bool(heartbeat_stats_sampling_prob) {
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

                    let elapsed = start.elapsed();
                    if elapsed.lt(&heartbeat_interval) {
                        sleep(heartbeat_interval.sub(elapsed));
                    }
                }
                sleep(Duration::from_millis(200)); // back off for a bit as client failed
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
