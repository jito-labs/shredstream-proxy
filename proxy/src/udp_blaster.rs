use std::{
    ffi::CString,
    mem::{self, MaybeUninit},
    net::{SocketAddr, UdpSocket},
    os::fd::AsRawFd,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

use clap::Parser;
use core_affinity;

use signal_hook::{consts::TERM_SIGNALS, iterator::Signals};

/// 1232 bytes (Solana transaction limit)
const SOLANA_TXN_BYTES: usize = 1232;

/// Max iov per sendmmsg
const MAX_IOV: usize = libc::UIO_MAXIOV as usize;

/// One call's max packets (tweak to taste; <= MAX_IOV)
const BURST: usize = 256;

#[derive(Parser, Debug)]
#[command(author, version, about = "Solana-sized UDP blaster using sendmmsg()")]
struct Opt {
    /// Destination, e.g. 198.18.0.2:20001
    #[arg(long)]
    dest: SocketAddr,

    /// Bind socket to a device (SO_BINDTODEVICE), e.g. enp1s0f0
    #[arg(long)]
    bind_device: Option<String>,

    /// DSCP/TOS (0-255)
    #[arg(long)]
    tos: Option<u8>,

    /// Threads (each thread = its own socket)
    #[arg(long, default_value_t = 1)]
    threads: usize,

    /// Comma-separated CPU cores for pinning (e.g. "0,2,4,6")
    #[arg(long)]
    cores: Option<String>,

    /// Seconds to run; 0 = until SIGINT/SIGTERM
    #[arg(long, default_value_t = 0)]
    duration: u64,

    /// Socket send buffer (bytes)
    #[arg(long, default_value_t = 64 * 1024 * 1024)]
    sndbuf: usize,

    /// Packets prebuilt per thread
    #[arg(long, default_value_t = 4096)]
    pool_pkts: usize,

    /// Packets per sendmmsg() call (<= UIO_MAXIOV)
    #[arg(long, default_value_t = BURST)]
    burst: usize,

    /// Build payload compatible with XDP blaster pattern (deterministic, first 8 bytes vary)
    #[arg(long, default_value_t = false)]
    xdp_compat: bool,
}

#[cfg(target_os = "linux")]
fn mmsghdr_for_packet(
    packet: &[u8],
    dest: &SocketAddr,
    iov: &mut MaybeUninit<libc::iovec>,
    addr: &mut MaybeUninit<libc::sockaddr_storage>,
    hdr: &mut MaybeUninit<libc::mmsghdr>,
) {
    use libc::{sockaddr_in, sockaddr_in6, socklen_t};
    const SIZE_OF_SOCKADDR_IN: usize = mem::size_of::<sockaddr_in>();
    const SIZE_OF_SOCKADDR_IN6: usize = mem::size_of::<sockaddr_in6>();
    const SIZE_OF_SOCKADDR_STORAGE: usize = mem::size_of::<libc::sockaddr_storage>();
    const SOCKADDR_IN_PADDING: usize = SIZE_OF_SOCKADDR_STORAGE - SIZE_OF_SOCKADDR_IN;
    const SOCKADDR_IN6_PADDING: usize = SIZE_OF_SOCKADDR_STORAGE - SIZE_OF_SOCKADDR_IN6;

    iov.write(libc::iovec {
        iov_base: packet.as_ptr() as *mut libc::c_void,
        iov_len: packet.len(),
    });

    let msg_namelen: socklen_t = match dest {
        SocketAddr::V4(a4) => {
            let ptr: *mut sockaddr_in = addr.as_mut_ptr() as *mut _;
            unsafe {
                let sockaddr = libc::sockaddr_in {
                    sin_family: libc::AF_INET as u16,
                    sin_port: a4.port().to_be(),
                    sin_addr: libc::in_addr {
                        s_addr: u32::from(*a4.ip()).to_be(),
                    },
                    sin_zero: [0; 8],
                };
                std::ptr::write(ptr, sockaddr);
                std::ptr::write_bytes(
                    (ptr as *mut u8).add(SIZE_OF_SOCKADDR_IN),
                    0,
                    SOCKADDR_IN_PADDING,
                );
            }
            SIZE_OF_SOCKADDR_IN as socklen_t
        }
        SocketAddr::V6(a6) => {
            let ptr: *mut sockaddr_in6 = addr.as_mut_ptr() as *mut _;
            unsafe {
                let sockaddr = libc::sockaddr_in6 {
                    sin6_family: libc::AF_INET6 as u16,
                    sin6_port: a6.port().to_be(),
                    sin6_flowinfo: 0,
                    sin6_addr: libc::in6_addr {
                        s6_addr: a6.ip().octets(),
                    },
                    sin6_scope_id: 0,
                };
                std::ptr::write(ptr, sockaddr);
                std::ptr::write_bytes(
                    (ptr as *mut u8).add(SIZE_OF_SOCKADDR_IN6),
                    0,
                    SOCKADDR_IN6_PADDING,
                );
            }
            SIZE_OF_SOCKADDR_IN6 as socklen_t
        }
    };

    let mut msghdr: libc::msghdr = unsafe { mem::zeroed() };
    // name
    msghdr.msg_name = addr.as_mut_ptr() as *mut libc::c_void;
    msghdr.msg_namelen = msg_namelen;
    // iov
    msghdr.msg_iov = iov.as_mut_ptr();
    msghdr.msg_iovlen = 1;
    // no ancillary data
    msghdr.msg_control = std::ptr::null_mut();
    msghdr.msg_controllen = 0;
    msghdr.msg_flags = 0;

    hdr.write(libc::mmsghdr { msg_hdr: msghdr, msg_len: 0 });
}

#[cfg(target_os = "linux")]
fn sendmmsg_retry(sock: &UdpSocket, hdrs: &mut [libc::mmsghdr]) {
    let fd = sock.as_raw_fd();
    let mut pkts = &mut *hdrs;
    while !pkts.is_empty() {
        let n = unsafe { libc::sendmmsg(fd, &mut pkts[0], pkts.len() as u32, 0) };
        match n {
            -1 => {
                // advance by one to skip a bad one, but don't die
                pkts = &mut pkts[1..];
            }
            sent => {
                let sent = sent as usize;
                pkts = &mut pkts[sent..];
            }
        }
    }
}

fn bind_to_device(sock: &UdpSocket, ifname: &str) -> std::io::Result<()> {
    let c = CString::new(ifname).unwrap();
    let ret = unsafe {
        libc::setsockopt(
            sock.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_BINDTODEVICE,
            c.as_ptr() as *const libc::c_void,
            c.as_bytes_with_nul().len() as libc::socklen_t,
        )
    };
    if ret == -1 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

fn set_udp_opts(sock: &UdpSocket, sndbuf: usize, tos: Option<u8>) {
    use std::os::unix::io::AsRawFd;
    
    // Set send buffer size using libc directly
    let fd = sock.as_raw_fd();
    let sndbuf_val = sndbuf as libc::c_int;
    unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_SNDBUF,
            &sndbuf_val as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
    }
    
    if let Some(t) = tos {
        let tos_val = t as libc::c_int;
        unsafe {
            libc::setsockopt(
                fd,
                libc::IPPROTO_IP,
                libc::IP_TOS,
                &tos_val as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
    }
    // Turn off UDP_GRO if present (Linux ≥ 6.1) - using libc directly
    #[cfg(target_os = "linux")]
    {
        let gro_val = 0i32;
        unsafe {
            // UDP_GRO is 104 on Linux, but may not be defined in older libc
            libc::setsockopt(
                fd,
                libc::SOL_UDP,
                104, // UDP_GRO
                &gro_val as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opt = Opt::parse();

    eprintln!(
        "[blaster] dest={} threads={} payload={}B pool={}, burst={}, bind_device={:?}",
        opt.dest,
        opt.threads,
        SOLANA_TXN_BYTES,
        opt.pool_pkts,
        opt.burst.min(MAX_IOV),
        opt.bind_device
    );

    let burst = opt.burst.min(MAX_IOV).max(1);

    // graceful shutdown
    let exit = Arc::new(AtomicBool::new(false));
    let mut sigs = Signals::new(TERM_SIGNALS)?;
    {
        let exit = exit.clone();
        thread::spawn(move || {
            for _ in &mut sigs {
                exit.store(true, Ordering::SeqCst);
            }
        });
    }
    if opt.duration > 0 {
        let exit = exit.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_secs(opt.duration));
            exit.store(true, Ordering::SeqCst);
        });
    }

    // build base payload template - each thread will modify it to create unique packets
    let base_payload: Vec<u8> = {
        if opt.xdp_compat {
            let mut v = vec![0xFEu8; SOLANA_TXN_BYTES];
            // We'll overwrite first 8 bytes per-packet with thread_id and packet_id (below)
            for i in 8..SOLANA_TXN_BYTES {
                v[i] = (i as u8).wrapping_mul(31).wrapping_add(0xAB);
            }
            v
        } else {
            let mut v = vec![0u8; SOLANA_TXN_BYTES];
            // simple pattern to avoid compression/TSO weirdness
            for (i, b) in v.iter_mut().enumerate() {
                *b = (i as u8).wrapping_mul(17).wrapping_add(0x5A);
            }
            v
        }
    };
    let base_payload = Arc::new(base_payload);

    // counters
    let total_pkts = Arc::new(AtomicU64::new(0));
    let total_bytes = Arc::new(AtomicU64::new(0));

    // CPU pin plan
    let core_plan: Vec<usize> = opt
        .cores
        .as_deref()
        .unwrap_or("")
        .split(',')
        .filter_map(|s| s.trim().parse::<usize>().ok())
        .collect();

    // spawn senders
    let mut handles = Vec::with_capacity(opt.threads + 1);
    for t in 0..opt.threads {
        let base_payload = base_payload.clone();
        let exit = exit.clone();
        let total_pkts = total_pkts.clone();
        let total_bytes = total_bytes.clone();
        let bind_dev = opt.bind_device.clone();
        let dest = opt.dest;
        let sndbuf = opt.sndbuf;
        let tos = opt.tos;
        let pool_pkts = opt.pool_pkts;
        let burst = burst;

        // choose a core if provided
        let pin_core = if core_plan.is_empty() {
            None
        } else {
            core_plan.get(t % core_plan.len()).copied()
        };

        let h = thread::Builder::new()
            .name(format!("sendmmsg-{}", t))
            .spawn(move || {
                if let Some(core_id) = pin_core {
                    if let Some(id) = core_affinity::get_core_ids()
                        .unwrap_or_default()
                        .into_iter()
                        .find(|c| c.id == core_id)
                    {
                        let _ = core_affinity::set_for_current(id);
                    }
                }

                // socket
                let sock = UdpSocket::bind(if dest.is_ipv4() { "0.0.0.0:0" } else { "[::]:0" })
                    .expect("bind socket");
                if let Some(dev) = bind_dev.as_ref() {
                    let _ = bind_to_device(&sock, dev);
                }
                set_udp_opts(&sock, sndbuf, tos);
                sock.connect(dest).expect("connect dest");

                // Build a pool of unique packets for this thread
                let mut packet_pool: Vec<Vec<u8>> = Vec::with_capacity(pool_pkts);
                for pkt_id in 0..pool_pkts {
                    let mut unique_packet = base_payload.as_ref().clone();
                    // Make each packet unique by embedding thread_id and packet_id in first 8 bytes
                    let thread_id = t as u32;
                    let packet_id = pkt_id as u32;
                    unique_packet[0..4].copy_from_slice(&thread_id.to_le_bytes());
                    unique_packet[4..8].copy_from_slice(&packet_id.to_le_bytes());
                    packet_pool.push(unique_packet);
                }
                let packet_refs: Vec<&[u8]> = packet_pool.iter().map(|p| p.as_slice()).collect();

                // Pre-allocated arrays for one sendmmsg() call (BURST packets)
                let mut iovs = vec![MaybeUninit::<libc::iovec>::uninit(); burst];
                let mut addrs = vec![MaybeUninit::<libc::sockaddr_storage>::uninit(); burst];
                let mut hdrs = vec![MaybeUninit::<libc::mmsghdr>::uninit(); burst];

                let mut idx = 0usize;
                let mut local_pkts = 0u64;
                let mut local_bytes = 0u64;

                while !exit.load(Ordering::Relaxed) {
                    // fill one burst
                    let mut used = 0usize;
                    for s in 0..burst {
                        let pkt = packet_refs[idx];
                        idx = (idx + 1) % packet_refs.len();
                        mmsghdr_for_packet(pkt, &dest, &mut iovs[s], &mut addrs[s], &mut hdrs[s]);
                        used += 1;
                    }

                    // SAFETY: first `used` entries are initialized
                    let hdrs_slice = unsafe {
                        std::slice::from_raw_parts_mut(hdrs.as_mut_ptr() as *mut libc::mmsghdr, used)
                    };

                    // send
                    sendmmsg_retry(&sock, hdrs_slice);

                    // account
                    local_pkts += used as u64;
                    local_bytes += (used * SOLANA_TXN_BYTES) as u64;

                    // drop init to keep UB gods happy; next loop we rewrite anyway
                    for k in 0..used {
                        unsafe {
                            hdrs[k].assume_init_drop();
                            iovs[k].assume_init_drop();
                            addrs[k].assume_init_drop();
                        }
                    }
                }

                total_pkts.fetch_add(local_pkts, Ordering::Relaxed);
                total_bytes.fetch_add(local_bytes, Ordering::Relaxed);
            })?;
        handles.push(h);
    }

    // reporter
    let total_pkts_r = total_pkts.clone();
    let total_bytes_r = total_bytes.clone();
    let exit_r = exit.clone();
    let reporter = thread::Builder::new()
        .name("metrics".into())
        .spawn(move || {
            let mut last_t = Instant::now();
            let mut last_p = 0_u64;
            let mut last_b = 0_u64;

            while !exit_r.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_secs(1));
                let now = Instant::now();
                let dt = now.duration_since(last_t).as_secs_f64();
                let p = total_pkts_r.load(Ordering::Relaxed);
                let b = total_bytes_r.load(Ordering::Relaxed);

                let dp = p.saturating_sub(last_p) as f64;
                let db = b.saturating_sub(last_b) as f64;

                let pps = dp / dt;
                let gbps = (db * 8.0) / (1e9 * dt);

                eprintln!("throughput: {:>10.0} pps | {:>6.2} Gbps", pps, gbps);

                last_t = now;
                last_p = p;
                last_b = b;
            }
        })?;
    handles.push(reporter);

    for h in handles {
        let _ = h.join();
    }

    eprintln!(
        "[blaster] done: {} packets, {} bytes",
        total_pkts.load(Ordering::Relaxed),
        total_bytes.load(Ordering::Relaxed)
    );
    Ok(())
}