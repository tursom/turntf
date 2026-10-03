//go:build linux

package cluster

import (
	"crypto/tls"
	"net"

	"github.com/tursom/turntf/internal/mesh"
	"golang.org/x/sys/unix"
)

// meshTCPConn 穿透 TLS 找到集群连接的 TCP socket；代理或其他连接类型返回 nil。
func meshTCPConn(c net.Conn) *net.TCPConn {
	for c != nil {
		switch v := c.(type) {
		case *net.TCPConn:
			return v
		case *tls.Conn:
			c = v.NetConn()
		default:
			return nil
		}
	}
	return nil
}

// setMeshTCPCongestion 为集群 TCP socket 指定拥塞控制算法；algo 为空时沿用系统默认。
func setMeshTCPCongestion(c net.Conn, algo string) error {
	tc := meshTCPConn(c)
	if algo == "" || tc == nil {
		return nil
	}
	raw, err := tc.SyscallConn()
	if err != nil {
		return err
	}
	var setErr error
	if err := raw.Control(func(fd uintptr) {
		setErr = unix.SetsockoptString(int(fd), unix.IPPROTO_TCP, unix.TCP_CONGESTION, algo)
	}); err != nil {
		return err
	}
	return setErr
}

// meshTCPInfo 读取集群 TCP socket 的内核统计。
func meshTCPInfo(c net.Conn) (mesh.TCPInfo, bool) {
	tc := meshTCPConn(c)
	if tc == nil {
		return mesh.TCPInfo{}, false
	}
	raw, err := tc.SyscallConn()
	if err != nil {
		return mesh.TCPInfo{}, false
	}
	var info *unix.TCPInfo
	var algo string
	var infoErr error
	if err := raw.Control(func(fd uintptr) {
		info, infoErr = unix.GetsockoptTCPInfo(int(fd), unix.IPPROTO_TCP, unix.TCP_INFO)
		algo, _ = unix.GetsockoptString(int(fd), unix.IPPROTO_TCP, unix.TCP_CONGESTION)
	}); err != nil || infoErr != nil {
		return mesh.TCPInfo{}, false
	}
	return mesh.TCPInfo{
		Congestion:      algo,
		RTTMicros:       info.Rtt,
		RTTVarMicros:    info.Rttvar,
		MinRTTMicros:    info.Min_rtt,
		SndCwnd:         info.Snd_cwnd,
		SndMSS:          info.Snd_mss,
		Unacked:         info.Unacked,
		NotSentBytes:    info.Notsent_bytes,
		BytesSent:       info.Bytes_sent,
		BytesAcked:      info.Bytes_acked,
		BytesRetrans:    info.Bytes_retrans,
		SegsOut:         info.Segs_out,
		TotalRetrans:    info.Total_retrans,
		DeliveryRateBps: info.Delivery_rate,
		PacingRateBps:   info.Pacing_rate,
	}, true
}
