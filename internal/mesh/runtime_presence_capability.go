package mesh

// SupportsPresenceDigest 只信任本进程直接握手得到的能力。多跳节点或连接
// 全部断开的节点返回 false，滚动升级时继续使用旧的权威分片修复。
func (r *Runtime) SupportsPresenceDigest(nodeID int64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	found := false
	for _, adj := range r.adjByConn {
		if adj.RemoteNodeID == nodeID {
			if !adj.Hello.GetPresenceDigestSupported() {
				return false
			}
			found = true
		}
	}
	return found
}
