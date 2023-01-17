use crate::token_authenticator::ClientInterceptor;
use jito_protos::shredstream::shredstream_client::ShredstreamClient;
use jito_protos::shredstream::Heartbeat;
use log::{info, warn};
use solana_metrics::datapoint_error;
use std::net::SocketAddr;
use std::ops::Sub;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{sleep, Builder, JoinHandle};
use std::time::{Duration, Instant};
use tokio::runtime::Runtime;
use tonic::codegen::InterceptedService;
use tonic::transport::Channel;

pub fn heartbeat_loop_thread(
    mut shredstream_client: ShredstreamClient<InterceptedService<Channel, ClientInterceptor>>,
    regions: Vec<String>,
    recv_socket: SocketAddr,
    exit: Arc<AtomicBool>,
) -> JoinHandle<()> {
    Builder::new()
        .name("heartbeat_loop".to_string())
        .spawn(move || {
            let mut successful_heartbeat_count: u64 = 0;
            let mut failed_heartbeat_count: u64 = 0;

            let heartbeat_socket = jito_protos::shared::Socket {
                ip: recv_socket.ip().to_string(),
                port: recv_socket.port() as i64,
            };

            let mut heartbeat_interval = Duration::from_millis(100); //start with 100ms, change based on server suggestion
            let rt = Runtime::new().unwrap();
            while !exit.load(Ordering::Relaxed) {
                let start = Instant::now();
                let result = rt.block_on(shredstream_client
                    .send_heartbeat(Heartbeat {
                        socket: Some(heartbeat_socket.clone()),
                        regions: regions.clone(),
                    }));

                match result {
                    Ok(x) => {
                        heartbeat_interval = Duration::from_millis(x.get_ref().ttl_ms as u64);
                        successful_heartbeat_count += 1;
                    }
                    Err(err) => {
                        warn!("Error sending heartbeat: {err}");
                        datapoint_error!("heartbeat_send_error", ("errors", 1, i64));
                        failed_heartbeat_count += 1;
                    }
                }
                let elapsed = start.elapsed();
                if elapsed.lt(&heartbeat_interval) {
                    sleep(heartbeat_interval.sub(elapsed));
                }
            }
            info!("Exiting heartbeat thread, sent {successful_heartbeat_count} successful, {failed_heartbeat_count} failed");
        })
        .unwrap()
}
