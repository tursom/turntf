package kv

import (
	"github.com/hashicorp/raft"
	"testing"
)

func TestValidateVoters(t *testing.T) {
	voters := []raft.Server{{ID: "1", Address: "1"}, {ID: "2", Address: "2"}, {ID: "3", Address: "3"}}
	if err := ValidateVoters("1", voters); err != nil {
		t.Fatal(err)
	}
	if err := ValidateVoters("1", voters[:2]); err == nil {
		t.Fatal("accepted even voter count")
	}
	if err := ValidateVoters("4", voters); err == nil {
		t.Fatal("accepted local node outside voters")
	}
}
