//go:build !linux

package cluster

import (
	"net"

	"github.com/tursom/turntf/internal/mesh"
)

// 非 Linux 平台不调整拥塞控制，也不提供 TCP_INFO。
func setMeshTCPCongestion(net.Conn, string) error { return nil }

func meshTCPInfo(net.Conn) (mesh.TCPInfo, bool) { return mesh.TCPInfo{}, false }
