package cluster

import (
	"fmt"
	"strconv"
)

func (c *Config) validateKVConsensus() error {
	cfg := c.KVConsensus
	if !cfg.Enabled {
		return nil
	}
	if cfg.GroupID == "" {
		return fmt.Errorf("kv consensus group_id cannot be empty")
	}
	if cfg.DataDir == "" {
		return fmt.Errorf("kv consensus data_dir cannot be empty")
	}
	if len(cfg.Voters) == 0 || len(cfg.Voters)%2 == 0 {
		return fmt.Errorf("kv consensus voters must be a non-zero odd number")
	}
	seen := make(map[int64]struct{}, len(cfg.Voters))
	found := false
	for _, raw := range cfg.Voters {
		id, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || id <= 0 {
			return fmt.Errorf("kv consensus voter %q is invalid", raw)
		}
		if _, ok := seen[id]; ok {
			return fmt.Errorf("kv consensus voter %d is duplicated", id)
		}
		seen[id] = struct{}{}
		if id == c.NodeID {
			found = true
		}
	}
	if !found {
		return fmt.Errorf("kv consensus local node %d is not a voter", c.NodeID)
	}
	for _, raw := range cfg.Learners {
		id, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || id <= 0 {
			return fmt.Errorf("kv consensus learner %q is invalid", raw)
		}
		if _, ok := seen[id]; ok {
			return fmt.Errorf("kv consensus learner %d overlaps voters", id)
		}
	}
	return nil
}
