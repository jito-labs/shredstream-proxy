use std::{
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket},
    process::Command,
};

use itertools::{Either, Itertools};
use log::{info, warn};
use serde::Deserialize;

fn run_ip_json(args: &[&str]) -> io::Result<Vec<u8>> {
    let output = Command::new("ip").args(args).output()?;
    if output.status.success() {
        Ok(output.stdout)
    } else {
        Err(io::Error::new(
            io::ErrorKind::Other,
            format!(
                "`ip {}` failed with status {}",
                args.join(" "),
                output.status
            ),
        ))
    }
}

/// Parse multicast groups routed to `device` via `ip --json route show dev <device>`
pub fn get_ip_route_for_device(device: &str) -> io::Result<Vec<IpAddr>> {
    let stdout = run_ip_json(&["--json", "route", "show", "dev", device])?;
    parse_ip_route_for_device(&stdout)
}

// Pure JSON parsers for unit testing
pub fn parse_ip_route_for_device(bytes: &[u8]) -> io::Result<Vec<IpAddr>> {
    #[derive(Debug, Deserialize)]
    struct RouteRow {
        dst: String,
    }

    let mut groups = serde_json::from_slice::<Vec<RouteRow>>(bytes)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
        .into_iter()
        .filter_map(|r| {
            if let Some((base, mask_str)) = r.dst.split_once('/') {
                let ip: IpAddr = base.parse().ok()?;
                let mask: u8 = mask_str.parse().ok()?;
                let is_exact = match ip {
                    IpAddr::V4(_) => mask == 32, // check if full-length mask (not partial)
                    IpAddr::V6(_) => mask == 128,
                };
                (ip.is_multicast() && is_exact).then_some(ip)
            } else {
                let ip: IpAddr = r.dst.parse().ok()?;
                ip.is_multicast().then_some(ip)
            }
        })
        .collect::<Vec<_>>();

    groups.sort_unstable();
    groups.dedup();
    Ok(groups)
}

/// Return the primary IPv4 address configured on `device` (if any), via `ip --json addr show`.
pub fn ipv4_addr_for_device(device: &str) -> io::Result<Option<Ipv4Addr>> {
    let stdout = run_ip_json(&["--json", "addr", "show", "dev", device])?;
    parse_ipv4_addr_from_ip_addr_show_json(&stdout)
}

/// Return the interface index for `device` (if any), via `ip --json link show`.
pub fn ifindex_for_device(device: &str) -> io::Result<Option<u32>> {
    let stdout = run_ip_json(&["--json", "link", "show", "dev", device])?;
    parse_ifindex_from_ip_link_show_json(&stdout)
}

// Pure JSON parsers for unit testing
pub fn parse_ipv4_addr_from_ip_addr_show_json(bytes: &[u8]) -> io::Result<Option<Ipv4Addr>> {
    #[derive(Debug, Deserialize)]
    struct AddrInfo {
        family: Option<String>,
        local: Option<String>,
    }
    #[derive(Debug, Deserialize)]
    struct IfaceRow {
        addr_info: Option<Vec<AddrInfo>>,
    }

    let rows: Vec<IfaceRow> =
        serde_json::from_slice(bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let ip = rows
        .into_iter()
        .flat_map(|row| row.addr_info.unwrap_or_default())
        .find_map(|info| {
            (info.family.as_deref() == Some("inet"))
                .then_some(info.local)
                .flatten()
        })
        .and_then(|s| s.parse::<Ipv4Addr>().ok());

    Ok(ip)
}

pub fn parse_ifindex_from_ip_link_show_json(bytes: &[u8]) -> io::Result<Option<u32>> {
    #[derive(Debug, Deserialize)]
    struct LinkRow {
        ifindex: Option<u32>,
    }

    let rows: Vec<LinkRow> =
        serde_json::from_slice(bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    Ok(rows.into_iter().last().and_then(|r| r.ifindex))
}

/// Creates one UDP socket bound on `multicast_port` and joins applicable multicast groups.
/// If `multicast_ip` is provided, join just that group, otherwise parse `ip route list` for
/// entries on `device_name` and join all multicast groups found.
pub fn create_multicast_socket_on_device(
    device_name: &str,
    multicast_port: u16,
    multicast_ip: Option<IpAddr>,
) -> Option<Vec<UdpSocket>> {
    let device_ipv4 = ipv4_addr_for_device(device_name).unwrap_or_else(|e| {
        warn!("Failed to resolve IPv4 address for device {device_name}: {e}");
        None
    });
    let device_ifindex_v6 = ifindex_for_device(device_name).unwrap_or_else(|e| {
        warn!("Failed to resolve ifindex for device {device_name}: {e}");
        None
    });

    let (groups_v4, groups_v6): (Vec<Ipv4Addr>, Vec<Ipv6Addr>) = match multicast_ip {
        Some(IpAddr::V4(g)) => (vec![g], Vec::new()),
        Some(IpAddr::V6(g6)) => (Vec::new(), vec![g6]),
        None => match get_ip_route_for_device(device_name) {
            Ok(ips) => ips.into_iter().partition_map(|ip| match ip {
                IpAddr::V4(v4) => Either::Left(v4),
                IpAddr::V6(v6) => Either::Right(v6),
            }),
            Err(e) => {
                warn!("Failed to parse 'ip route list' for {device_name}: {e}");
                (Vec::new(), Vec::new())
            }
        },
    };

    if groups_v4.is_empty() && groups_v6.is_empty() {
        warn!("No multicast groups found for device {device_name}; skipping multicast listener");
        return None;
    }

    let mut sockets: Vec<UdpSocket> = Vec::new();
    if !groups_v4.is_empty() {
        let addr_v4 = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), multicast_port);
        match UdpSocket::bind(addr_v4) {
            Ok(sock_v4) => {
                for g in &groups_v4 {
                    match sock_v4
                        .join_multicast_v4(g, &device_ipv4.unwrap_or(Ipv4Addr::UNSPECIFIED))
                    {
                        Ok(()) => info!("Joined IPv4 multicast group {g} port {multicast_port}"),
                        Err(e) => warn!("Failed joining IPv4 group {g}: {e}"),
                    }
                }
                sockets.push(sock_v4);
            }
            Err(e) => warn!("Failed to bind IPv4 multicast socket on {addr_v4}: {e}"),
        }
    }

    if !groups_v6.is_empty() {
        let addr_v6 = SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), multicast_port);
        match UdpSocket::bind(addr_v6) {
            Ok(sock_v6) => {
                for g in &groups_v6 {
                    match sock_v6.join_multicast_v6(g, device_ifindex_v6.unwrap_or(0)) {
                        Ok(()) => info!("Joined IPv6 multicast group {g} port {multicast_port}"),
                        Err(e) => warn!("Failed joining IPv6 group {g}: {e}"),
                    }
                }
                sockets.push(sock_v6);
            }
            Err(e) => warn!("Failed to bind IPv6 multicast socket on {addr_v6}: {e}"),
        }
    }

    (!sockets.is_empty()).then_some(sockets)
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use super::{
        parse_ifindex_from_ip_link_show_json, parse_ip_route_for_device,
        parse_ipv4_addr_from_ip_addr_show_json,
    };

    #[test]
    fn parse_ip_route_for_device_test() {
        let json = r#"[{"dst":"169.254.2.112/31","protocol":"kernel","scope":"link","prefsrc":"169.254.2.113","flags":[]},{"dst":"233.84.178.2","gateway":"169.254.2.112","protocol":"static","flags":[]}]"#;
        let parsed = parse_ip_route_for_device(json.as_bytes()).unwrap();
        assert_eq!(parsed, vec![Ipv4Addr::new(233, 84, 178, 2)]);
    }

    #[test]
    fn parse_ipv4_addr_from_addr() {
        let json = r#"[
            {"addr_info":[
                {"family":"inet6","local":"fe80::1234"},
                {"family":"inet","local":"192.168.1.10"}
            ]}
        ]"#;
        let parsed = parse_ipv4_addr_from_ip_addr_show_json(json.as_bytes()).unwrap();
        assert_eq!(parsed, Some(Ipv4Addr::new(192, 168, 1, 10)));
    }

    #[test]
    fn parse_ipv4_addr_from_addr_show_malformed() {
        let json = r#"{"not":"an array"}"#;
        let res = parse_ipv4_addr_from_ip_addr_show_json(json.as_bytes());
        assert!(res.is_err());
    }

    #[test]
    fn parse_ifindex_from_link_show_present() {
        let json = r#"[
            {"ifindex":3,"ifname":"lo"}
        ]"#;
        let parsed = parse_ifindex_from_ip_link_show_json(json.as_bytes()).unwrap();
        assert_eq!(parsed, Some(3));
    }

    #[test]
    fn parse_ifindex_from_link_show_empty() {
        let json = r#"[]"#;
        let parsed = parse_ifindex_from_ip_link_show_json(json.as_bytes()).unwrap();
        assert_eq!(parsed, None);
    }

    #[test]
    fn parse_ifindex_from_link_show_malformed() {
        let json = r#"{"ifindex":3}"#;
        let res = parse_ifindex_from_ip_link_show_json(json.as_bytes());
        assert!(res.is_err());
    }
}
