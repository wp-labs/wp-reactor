//! CIDR 子网匹配（Sigma `|cidr` 修饰符等效）。
//!
//! 仅依赖 std 的 IP 解析（IPv4 + IPv6），掩码比较用整数位运算完成，不引入
//! 第三方网络库。行为对齐常见实现：
//! - `"10.0.0.0/8"` 标准网络地址；
//! - 主机位非零的写法（如 `"10.1.2.3/8"`）按网络地址处理（自动掩掉主机位）；
//! - 前缀 0 = 默认路由（匹配任意地址）。
//!
//! 供两处使用：`checker/types/check_funcs.rs` 编译期校验 `cidr_match` 的
//! 子网字面量；`wf-engine` 两个求值路径运行时做判定。

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

/// 解析后的 CIDR 网络（按网络地址存储，主机位清零）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, ::moju_derive::MoJu)]
#[moju(kind = "struct", domain = "Lang", module = "Lang.LangCidr")]
pub struct Cidr {
    net: IpAddr,
    prefix: u8,
}

impl Cidr {
    /// 解析 `"addr/prefix"`。格式非法 / 前缀越界 → `None`。
    /// 主机位非零的写法会被掩成网络地址，因此 `"10.1.2.3/8"` 与 `"10.0.0.0/8"` 等价。
    pub fn parse(s: &str) -> Option<Self> {
        let (addr, prefix) = s.trim().split_once('/')?;
        let ip: IpAddr = addr.trim().parse().ok()?;
        let prefix: u8 = prefix.trim().parse().ok()?;
        match ip {
            IpAddr::V4(_) if prefix > 32 => return None,
            IpAddr::V6(_) if prefix > 128 => return None,
            _ => {}
        }
        Some(Cidr {
            net: masked(ip, prefix),
            prefix,
        })
    }

    /// `ip` 是否落在这个子网内（版本不一致 → 不匹配）。
    pub fn contains(&self, ip: &str) -> bool {
        let ip: IpAddr = match ip.trim().parse() {
            Ok(ip) => ip,
            Err(_) => return false,
        };
        match (self.net, ip) {
            (IpAddr::V4(net), IpAddr::V4(ip)) => {
                let mask = v4_mask(self.prefix);
                (u32::from(ip) & mask) == (u32::from(net) & mask)
            }
            (IpAddr::V6(net), IpAddr::V6(ip)) => {
                let mask = v6_mask(self.prefix);
                (u128::from(ip) & mask) == (u128::from(net) & mask)
            }
            _ => false,
        }
    }
}

fn v4_mask(prefix: u8) -> u32 {
    if prefix == 0 {
        0
    } else {
        u32::MAX << (32 - prefix)
    }
}

fn v6_mask(prefix: u8) -> u128 {
    if prefix == 0 {
        0
    } else {
        u128::MAX << (128 - prefix)
    }
}

fn masked(ip: IpAddr, prefix: u8) -> IpAddr {
    match ip {
        IpAddr::V4(v4) => IpAddr::V4(Ipv4Addr::from(u32::from(v4) & v4_mask(prefix))),
        IpAddr::V6(v6) => IpAddr::V6(Ipv6Addr::from(u128::from(v6) & v6_mask(prefix))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v4_private_ranges() {
        let net = Cidr::parse("10.0.0.0/8").unwrap();
        assert!(net.contains("10.1.2.3"));
        assert!(!net.contains("11.0.0.1"));
        assert!(net.contains("10.255.255.255"));
    }

    #[test]
    fn v4_host_bits_masked() {
        // 主机位非零写法与标准网络地址等价。
        let a = Cidr::parse("10.1.2.3/8").unwrap();
        let b = Cidr::parse("10.0.0.0/8").unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn v4_slash_boundary() {
        let n24 = Cidr::parse("192.168.1.0/24").unwrap();
        assert!(n24.contains("192.168.1.254"));
        assert!(!n24.contains("192.168.2.1"));

        let n32 = Cidr::parse("8.8.8.8/32").unwrap();
        assert!(n32.contains("8.8.8.8"));
        assert!(!n32.contains("8.8.8.9"));

        // 默认路由 /0 匹配所有 v4。
        let all = Cidr::parse("0.0.0.0/0").unwrap();
        assert!(all.contains("1.2.3.4"));
    }

    #[test]
    fn v6_support() {
        let loopback = Cidr::parse("::1/128").unwrap();
        assert!(loopback.contains("::1"));
        assert!(!loopback.contains("::2"));

        let link_local = Cidr::parse("fe80::/10").unwrap();
        assert!(link_local.contains("fe80::1"));
        assert!(!link_local.contains("fe00::1"));

        // v6 不匹配 v4（版本不一致）。
        assert!(!loopback.contains("127.0.0.1"));
    }

    #[test]
    fn invalid_inputs() {
        assert!(Cidr::parse("").is_none());
        assert!(Cidr::parse("10.0.0.0").is_none()); // 无前缀
        assert!(Cidr::parse("10.0.0.0/33").is_none()); // 前缀越界
        assert!(Cidr::parse("10.0.0.0/x").is_none());
        assert!(Cidr::parse("not-an-ip/8").is_none());
        assert!(Cidr::parse("::/129").is_none());
        assert!(Cidr::parse("10.0.0.0/").is_none());
    }
}
