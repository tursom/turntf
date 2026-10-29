package mesh

// SupportsConsensusBinaryV1 only trusts direct hello exchanges. During a mixed
// connection upgrade every connection must agree, since routing can select any
// of them. Unknown or disconnected peers use the legacy JSON format.
func (r *Runtime) SupportsConsensusBinaryV1(nodeID int64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	found := false
	for _, adj := range r.adjByConn {
		if adj.RemoteNodeID == nodeID {
			if !adj.Hello.GetConsensusBinaryV1Supported() {
				return false
			}
			found = true
		}
	}
	return found
}
