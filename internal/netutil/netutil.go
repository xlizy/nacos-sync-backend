// Package netutil 提供本机 IP 探测工具，供「同步服务自注册」与「代注册实例溯源」复用，
// 保证两者取到的本机 IP 口径一致（排查时 NACOS-SYNC 注册 IP 与代注册 metadata 的 syncSourceAgent 指向同一台机器）。
package netutil

import (
	"fmt"
	"net"
)

// OutboundIP 返回「能路由到 target（如 Nacos 服务器地址 host:port）」的本机 IPv4 地址。
// 用 UDP 拨号做路由表查询（不发送真实报文），得到的本地地址即其他同网段服务访问本后端应使用的 IP。
func OutboundIP(target string) (string, error) {
	conn, err := net.Dial("udp", target)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	localAddr, ok := conn.LocalAddr().(*net.UDPAddr)
	if !ok {
		return "", fmt.Errorf("unexpected local addr type %T", conn.LocalAddr())
	}
	ip4 := localAddr.IP.To4()
	if ip4 == nil {
		return "", fmt.Errorf("local addr is not ipv4: %s", localAddr.IP)
	}
	return ip4.String(), nil
}

// FirstNonLoopbackIP 兜底：返回首个非 loopback 的 IPv4 地址。
func FirstNonLoopbackIP() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	for _, a := range addrs {
		ipNet, ok := a.(*net.IPNet)
		if !ok || ipNet.IP.IsLoopback() {
			continue
		}
		if ip4 := ipNet.IP.To4(); ip4 != nil {
			return ip4.String(), nil
		}
	}
	return "", fmt.Errorf("no non-loopback ipv4 address found")
}

// LocalIP 返回可用于标识本机的 IPv4 地址：优先取「能路由到 target 的本机地址」，
// 失败回退到首个非 loopback 地址，再不行用 127.0.0.1。target 一般为 Nacos 服务器地址。
func LocalIP(target string) string {
	if ip, err := OutboundIP(target); err == nil {
		return ip
	}
	if ip, err := FirstNonLoopbackIP(); err == nil {
		return ip
	}
	return "127.0.0.1"
}
