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

#[allow(clippy::too_many_arguments)]
pub fn heartbeat_loop_thread(
    block_engine_url: String,
    auth_url: String,
    auth_keypair: Arc<Keypair>,
    desired_regions: Vec<String>,
    recv_socket: SocketAddr,
    runtime: Runtime,
    service_name: String,
    grpc_restart_signal: Receiver<()>,
    shutdown_receiver: Receiver<()>,
    exit: Arc<AtomicBool>,
) -> JoinHandle<()> {
    Builder::new().name("ssPxyHbeatLoop".to_string()).spawn(move || {
        let heartbeat_socket = jito_protos::shared::Socket {
            ip: recv_socket.ip().to_string(),
            port: recv_socket.port() as i64,
        };
        let mut heartbeat_interval = Duration::from_secs(1); //start with 1s, change based on server suggestion
        // use tick() since we want to avoid thread::sleep(), as it's not interruptable. want to be interruptable for exiting quickly
        let mut heartbeat_tick = crossbeam_channel::tick(heartbeat_interval);
        let metrics_tick = crossbeam_channel::tick(Duration::from_secs(30));
        let mut client_restart_count = 0u64;
        let mut successful_heartbeat_count = 0u64;
        let mut failed_heartbeat_count = 0u64;
        let mut client_restart_count_cumulative = 0u64;
        let mut successful_heartbeat_count_cumulative = 0u64;
        let mut failed_heartbeat_count_cumulative = 0u64;

        while !exit.load(Ordering::Relaxed) {
            info!("Starting heartbeat client");
            let shredstream_client_res = runtime.block_on(
                get_grpc_client(
                    block_engine_url.clone(),
                    auth_url.clone(),
                    auth_keypair.clone(),
                    service_name.clone(),
                    exit.clone()
                )
            );
            let (mut shredstream_client , refresh_thread_hdl) = match shredstream_client_res {
                Ok(c) => c,
                Err(e) => {
                    warn!("Failed to connect to block engine, retrying. Error: {e}");
                    client_restart_count += 1;
                    datapoint_warn!(
                        "shredstream_proxy-heartbeat_client_error",
                        "block_engine_url" => block_engine_url,
                        ("errors", 1, i64),
                        ("error_str", e.to_string(), String),
                    );
                    sleep(Duration::from_secs(5));
                    continue; // avoid sending heartbeat, try acquiring grpc client again
                }
            };

            while !exit.load(Ordering::Relaxed) {
                crossbeam_channel::select! {
                    // send heartbeat
                    recv(heartbeat_tick) -> _ => {
                        let heartbeat_result = runtime.block_on(shredstream_client
                            .send_heartbeat(Heartbeat {
                                socket: Some(heartbeat_socket.clone()),
                                regions: desired_regions.clone(),
                            }));

                        match heartbeat_result {
                            Ok(hb) => {
                                // retry sooner in case a heartbeat fails
                                let new_interval = Duration::from_millis((hb.get_ref().ttl_ms / 3) as u64);
                                if heartbeat_interval != new_interval {
                                    info!("Sending heartbeat every {new_interval:?}.");
                                    heartbeat_interval = new_interval;
                                    heartbeat_tick = crossbeam_channel::tick(new_interval);
                                }
                                successful_heartbeat_count += 1;
                            }
                            Err(err) => {
                                if err.code() == Code::InvalidArgument {
                                    panic!("Invalid arguments: {err}.");
                                };
                                warn!("Error sending heartbeat: {err}");
                                datapoint_warn!(
                                    "shredstream_proxy-heartbeat_send_error",
                                    "block_engine_url" => block_engine_url,
                                    ("errors", 1, i64),
                                    ("error_str", err.to_string(), String),
                                );
                                failed_heartbeat_count += 1;
                            }
                        }
                    }
                    // send metrics
                    recv(metrics_tick) -> _ => {
                        datapoint_info!(
                            "shredstream_proxy-heartbeat_stats",
                            "block_engine_url" => block_engine_url,
                            ("successful_heartbeat_count", successful_heartbeat_count, i64),
                            ("failed_heartbeat_count", failed_heartbeat_count, i64),
                            ("client_restart_count", client_restart_count, i64),
                        );
                        successful_heartbeat_count_cumulative += successful_heartbeat_count;
                        failed_heartbeat_count_cumulative += failed_heartbeat_count;
                        client_restart_count_cumulative += client_restart_count;
                        successful_heartbeat_count = 0;
                        failed_heartbeat_count = 0;
                        client_restart_count = 0;
                    }
                    // restart grpc client if no shreds received
                    recv(grpc_restart_signal) -> _ => {
                        refresh_thread_hdl.abort();
                        warn!("No shreds received recently, restarting heartbeat client.");
                        datapoint_warn!(
                            "shredstream_proxy-heartbeat_restart_signal",
                            "block_engine_url" => block_engine_url,
                            ("desired_regions", format!("{desired_regions:?}"), String),
                            ("client_restart_count", client_restart_count, i64),
                        );
                        // exit should be false
                        break;
                    }
                    // handle SIGINT shutdown
                    recv(shutdown_receiver) -> _ => {
                        // exit should be true
                        break;
                    }
                }
            }
        }
        info!("Exiting heartbeat thread, sent {successful_heartbeat_count_cumulative} successful, {failed_heartbeat_count_cumulative} failed heartbeats. Client restarted {client_restart_count_cumulative} times.");
    }).unwrap()
}

pub async fn get_grpc_client(
    block_engine_url: String,
    auth_url: String,
    auth_keypair: Arc<Keypair>,
    service_name: String,
    exit: Arc<AtomicBool>,
) -> Result<
    (
        ShredstreamClient<InterceptedService<Channel, ClientInterceptor>>,
        tokio::task::JoinHandle<()>,
    ),
    ShredstreamProxyError,
> {
    let auth_channel = create_grpc_channel(auth_url).await?;
    let searcher_channel = create_grpc_channel(block_engine_url).await?;
    let (client_interceptor, thread_handle) = ClientInterceptor::new(
        AuthServiceClient::new(auth_channel),
        auth_keypair,
        Role::ShredstreamSubscriber,
        service_name,
        exit,
    )
    .await?;
    let searcher_client = ShredstreamClient::with_interceptor(searcher_channel, client_interceptor);
    Ok((searcher_client, thread_handle))
}
