use {
    agave_xdp::{
        device::{NetworkDevice, QueueId},
        load_xdp_program, set_cpu_affinity,
        tx_loop::tx_loop,
    },
    caps::{CapSet, Capability},
    clap::Parser,
    std::{
        hint,
        net::{IpAddr, SocketAddr},
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Arc,
        },
        thread,
        time::{Duration, Instant},
    },
};
 
#[derive(Parser, Debug)]
#[command(author, version, about = "AF_XDP UDP sender", long_about = None)]
struct Opt {
    #[arg(short, long, default_value = "enp1s0f1")]
    interface: String,
 
    #[arg(long, default_value = "127.0.0.1")]
    dest_ip: String,
 
    #[arg(long, default_value = "20000")]
    dest_port: u16,
 
    #[arg(short, long, default_value = "64")]
    payload_size: usize,
 
    #[arg(short, long, default_value = "1000")]
    batch_size: usize,
 
    #[arg(long, default_value = "0")]
    idle_sleep_us: u64,
 
    #[arg(long, default_value = "0")]
    churn_threads: usize,
 
    #[arg(short, long)]
    zero_copy: bool,
}
 
// Metrics structure to share between threads
struct Metrics {
    tx_packets: AtomicUsize,
    tx_bytes: AtomicUsize,
}
 
// Helper function to format bitrate with appropriate units
fn format_bitrate(bits_per_second: f64) -> String {
    if bits_per_second < 1000.0 {
        format!("{:.2} bps", bits_per_second)
    } else if bits_per_second < 1_000_000.0 {
        format!("{:.2} Kbps", bits_per_second / 1000.0)
    } else if bits_per_second < 1_000_000_000.0 {
        format!("{:.2} Mbps", bits_per_second / 1_000_000.0)
    } else {
        format!("{:.2} Gbps", bits_per_second / 1_000_000_000.0)
    }
}
 
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opt = Opt::parse();
 
    let exit = Arc::new(AtomicBool::new(false));
 
    // Use ctrlc for graceful shutdown
    ctrlc::set_handler({
        let exit = Arc::clone(&exit);
        move || {
            println!("exiting...");
            exit.store(true, Ordering::Relaxed);
        }
    })?;
 
    for cap in [
        Capability::CAP_NET_ADMIN,
        Capability::CAP_NET_RAW,
        Capability::CAP_BPF,
    ] {
        caps::raise(None, CapSet::Effective, cap).unwrap();
    }
 
    let mut cores = core_affinity::get_core_ids()
        .expect("Failed to get core IDs")
        .into_iter()
        .map(|id| id.id)
        .collect::<Vec<_>>();
    cores.remove(2);
 
    set_cpu_affinity(cores).unwrap();
 
    for _ in 0..opt.churn_threads {
        thread::spawn(|| loop {
            hint::black_box(())
        });
    }
 
    set_cpu_affinity([2]).unwrap();
 
    for cap in [
        Capability::CAP_NET_ADMIN,
        Capability::CAP_NET_RAW,
        Capability::CAP_BPF,
    ] {
        caps::drop(None, CapSet::Effective, cap).unwrap();
    }
 
    let metrics = Arc::new(Metrics {
        tx_packets: AtomicUsize::new(0),
        tx_bytes: AtomicUsize::new(0),
    });
 
    let metrics_thread = thread::Builder::new()
        .name("metrics".to_string())
        .spawn({
            let metrics = metrics.clone();
            let exit = Arc::clone(&exit);
            move || {
                let mut last_time = Instant::now();
                let mut last_packets = 0;
                let mut last_bytes = 0;
 
                while exit.load(Ordering::SeqCst) == false {
                    thread::sleep(Duration::from_secs(1));
 
                    let current_time = Instant::now();
                    let elapsed = current_time.duration_since(last_time).as_secs_f64();
 
                    // Get current metrics from shared metrics
                    let packets = metrics.tx_packets.load(Ordering::SeqCst);
                    let bytes = metrics.tx_bytes.load(Ordering::SeqCst);
 
                    let pps = (packets - last_packets) as f64 / elapsed;
                    let bps = ((bytes - last_bytes) as f64 * 8.0) / elapsed;
 
                    println!("throughput: {:.2} pps | {}", pps, format_bitrate(bps));
 
                    last_time = current_time;
                    last_packets = packets;
                    last_bytes = bytes;
                }
            }
        })
        .unwrap();
 
    let udp_payload_size = opt.payload_size;
    let packet_size = 14 + 20 + 8 + udp_payload_size; // Eth + IP + UDP + payload
 
    println!("sending UDP packets to {}:{} (proxy port)", opt.dest_ip, opt.dest_port);
 
    let mut packet_data = vec![0xFEu8; packet_size];
    for i in 0..udp_payload_size {
        packet_data[14 + 20 + 8 + i] = (i % 256) as u8;
    }
 
    let packet_data = Packet(Arc::new(packet_data[14 + 20 + 8..].to_vec()));
 
    let (drop_sender, drop_receiver) = crossbeam_channel::bounded::<(Addrs, Packet)>(2_000_000);
    let drop_thread = thread::spawn(move || {
        loop {
            match drop_receiver.try_recv() {
                Ok((addrs, data)) => {
                    metrics
                        .tx_packets
                        .fetch_add(addrs.as_ref().len(), Ordering::Relaxed);
                    metrics.tx_bytes.fetch_add(
                        addrs.as_ref().len() * data.as_ref().len(),
                        Ordering::Relaxed,
                    );
                }
                Err(crossbeam_channel::TryRecvError::Empty) => {
                    // no frames to drop, just sleep for a bit
                    thread::sleep(Duration::from_millis(1));
                }
                Err(crossbeam_channel::TryRecvError::Disconnected) => {
                    // channel is closed, exit the loop
                    break;
                }
            };
        }
    });
 
    // Use the requested NIC and queue 0 by default
    let nic_name = opt.interface.clone();
    let interfaces: Vec<(&str, usize)> = vec![(nic_name.as_str(), 0)];
    let (_devs, _xdps) = interfaces
        .iter()
        .map(|(iface, _)| {
            let dev = NetworkDevice::new(*iface).unwrap();
            let ebpf = if opt.zero_copy {
                Some(load_xdp_program(dev.if_index()).unwrap())
            } else {
                None
            };
            (dev, ebpf)
        })
        .unzip::<_, _, Vec<_>, Vec<_>>();
    let tx_loops = (0..1usize)
        .into_iter()
        .map(|i| {
            let (iface, cpu) = interfaces[i];
            let dev = NetworkDevice::new(iface).unwrap();
            let drop_sender = drop_sender.clone();
            let (sender, receiver) = crossbeam_channel::bounded(500_000);
            (
                sender,
                thread::Builder::new()
                    .name("agave_xdp_tx_loop".to_string())
                    .spawn(move || {
                        tx_loop(
                            cpu,
                            &dev,
                            QueueId(i as u64),
                            opt.zero_copy,
                            None,
                            // Let the library pick the NIC's IPv4 address
                            None,
                            1234,
                            dev.mac_addr().ok(),
                            receiver,
                            drop_sender,
                        );
                    })
                    .unwrap(),
            )
        })
        .collect::<Vec<_>>();
    drop(drop_sender);
 
    let addrs = Addrs(Arc::new(vec![
        (
            opt.dest_ip.parse::<IpAddr>().unwrap(),
            opt.dest_port
        )
            .into();
        1
    ]));
 
    for (i, (sender, _)) in tx_loops.iter().cycle().enumerate() {
        if exit.load(Ordering::Relaxed) {
            break;
        }
        if let Err(_) = sender.try_send((addrs.clone(), packet_data.clone())) {
            if i > 0 && i % tx_loops.len() == 0 {
                // thread::sleep(Duration::from_micros(500));
            }
        }
    }
 
    for (i, (sender, tx_loop)) in tx_loops.into_iter().enumerate() {
        drop(sender);
        eprintln!("joining {i}");
        tx_loop.join().unwrap();
    }
    drop_thread.join().unwrap();
    metrics_thread.join().unwrap();
 
    println!("terminated");
    Ok(())
}
 
#[derive(Clone)]
struct Addrs(Arc<Vec<SocketAddr>>);
 
impl AsRef<[SocketAddr]> for Addrs {
    fn as_ref(&self) -> &[SocketAddr] {
        &self.0
    }
}
 
#[derive(Clone)]
struct Packet(Arc<Vec<u8>>);
 
impl AsRef<[u8]> for Packet {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}