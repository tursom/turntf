package kv

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/hashicorp/raft"
	pb "github.com/tursom/turntf/internal/proto"
	"google.golang.org/protobuf/proto"
)

var binaryRPCPrefix = []byte{'T', 'F', 'R', 1}

// Match the common mesh frame budget. Envelope overhead can impose a smaller
// limit; larger snapshots require chunking rather than unbounded buffering.
const maxConsensusSnapshotBytes = 8 << 20

type rpcWire struct {
	ID       uint64          `json:"id"`
	Type     string          `json:"type"`
	Source   string          `json:"source,omitempty"`
	Request  json.RawMessage `json:"request,omitempty"`
	Response json.RawMessage `json:"response,omitempty"`
	Error    string          `json:"error,omitempty"`

	binary   bool
	response bool
	value    any
	snapshot []byte
}

func rpcValue(method string, response bool) (any, error) {
	if response {
		switch method {
		case "append":
			return &raft.AppendEntriesResponse{}, nil
		case "vote":
			return &raft.RequestVoteResponse{}, nil
		case "prevote":
			return &raft.RequestPreVoteResponse{}, nil
		case "snapshot":
			return &raft.InstallSnapshotResponse{}, nil
		case "timeout":
			return &raft.TimeoutNowResponse{}, nil
		}
	} else {
		switch method {
		case "append":
			return &raft.AppendEntriesRequest{}, nil
		case "vote":
			return &raft.RequestVoteRequest{}, nil
		case "prevote":
			return &raft.RequestPreVoteRequest{}, nil
		case "snapshot":
			return &raft.InstallSnapshotRequest{}, nil
		case "timeout":
			return &raft.TimeoutNowRequest{}, nil
		}
	}
	return nil, fmt.Errorf("kv: unknown consensus rpc %q", method)
}

func encodeRPC(w rpcWire) ([]byte, error) {
	if !w.binary {
		value, err := json.Marshal(w.value)
		if err != nil {
			return nil, err
		}
		if w.response {
			w.Response = value
		} else {
			w.Request = value
		}
		return json.Marshal(w)
	}
	out := &pb.RaftRPC{Id: w.ID, Method: w.Type, Source: w.Source, Error: w.Error}
	if w.response {
		response, err := encodeRaftResponse(w.value)
		if err != nil {
			return nil, err
		}
		out.Body = &pb.RaftRPC_Response{Response: response}
	} else {
		request, err := encodeRaftRequest(w.value)
		if err != nil {
			return nil, err
		}
		request.SnapshotData = w.snapshot
		out.Body = &pb.RaftRPC_Request{Request: request}
	}
	b, err := proto.Marshal(out)
	if err != nil {
		return nil, err
	}
	return append(append([]byte(nil), binaryRPCPrefix...), b...), nil
}

func decodeRPC(payload []byte) (rpcWire, error) {
	var w rpcWire
	if bytes.HasPrefix(payload, []byte("TFR")) {
		if !bytes.HasPrefix(payload, binaryRPCPrefix) {
			return w, errors.New("kv: unsupported consensus codec version")
		}
		var in pb.RaftRPC
		if err := proto.Unmarshal(payload[len(binaryRPCPrefix):], &in); err != nil {
			return w, err
		}
		w.ID, w.Type, w.Source, w.Error, w.binary = in.Id, in.Method, in.Source, in.Error, true
		var err error
		switch body := in.Body.(type) {
		case *pb.RaftRPC_Request:
			w.value, err = decodeRaftRequest(w.Type, body.Request)
			w.snapshot = body.Request.GetSnapshotData()
		case *pb.RaftRPC_Response:
			w.response = true
			w.value, err = decodeRaftResponse(w.Type, body.Response)
		default:
			err = errors.New("kv: missing consensus body")
		}
		if err != nil {
			return w, err
		}
	} else {
		if err := json.Unmarshal(payload, &w); err != nil {
			return w, err
		}
		if (len(w.Request) == 0) == (len(w.Response) == 0) {
			return w, errors.New("kv: ambiguous consensus direction")
		}
		w.response = len(w.Response) != 0
		value, err := rpcValue(w.Type, w.response)
		if err != nil {
			return w, err
		}
		raw := w.Request
		if w.response {
			raw = w.Response
		}
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return w, errors.New("kv: null consensus body")
		}
		if err := json.Unmarshal(raw, value); err != nil {
			return w, err
		}
		w.value = value
	}
	if w.ID == 0 {
		return w, errors.New("kv: consensus message id is zero")
	}
	if !w.response && (w.Source == "" || w.Error != "") {
		return w, errors.New("kv: invalid consensus request")
	}
	return w, nil
}

func encodeRaftHeader(h raft.RPCHeader) *pb.RaftHeader {
	return &pb.RaftHeader{ProtocolVersion: int64(h.ProtocolVersion), Id: h.ID, Addr: h.Addr}
}

func decodeRaftHeader(h *pb.RaftHeader) (raft.RPCHeader, error) {
	if h == nil {
		return raft.RPCHeader{}, errors.New("kv: missing raft header")
	}
	version := raft.ProtocolVersion(h.ProtocolVersion)
	if int64(version) != h.ProtocolVersion || version < 0 {
		return raft.RPCHeader{}, errors.New("kv: invalid raft protocol version")
	}
	return raft.RPCHeader{ProtocolVersion: version, ID: h.Id, Addr: h.Addr}, nil
}

func encodeRaftRequest(value any) (*pb.RaftRequest, error) {
	q := &pb.RaftRequest{}
	switch v := value.(type) {
	case *raft.AppendEntriesRequest:
		q.Header, q.Term, q.Leader = encodeRaftHeader(v.RPCHeader), v.Term, v.Leader
		q.PrevLogEntry, q.PrevLogTerm, q.LeaderCommitIndex = v.PrevLogEntry, v.PrevLogTerm, v.LeaderCommitIndex
		for _, log := range v.Entries {
			if log == nil {
				return nil, errors.New("kv: nil raft log")
			}
			entry := &pb.RaftLog{Index: log.Index, Term: log.Term, Type: uint32(log.Type), Data: log.Data, Extensions: log.Extensions}
			if !log.AppendedAt.IsZero() {
				var err error
				entry.AppendedAt, err = log.AppendedAt.MarshalBinary()
				if err != nil {
					return nil, err
				}
			}
			q.Entries = append(q.Entries, entry)
		}
	case *raft.RequestVoteRequest:
		q.Header, q.Term, q.Candidate = encodeRaftHeader(v.RPCHeader), v.Term, v.Candidate
		q.LastLogIndex, q.LastLogTerm, q.LeadershipTransfer = v.LastLogIndex, v.LastLogTerm, v.LeadershipTransfer
	case *raft.RequestPreVoteRequest:
		q.Header, q.Term, q.LastLogIndex, q.LastLogTerm = encodeRaftHeader(v.RPCHeader), v.Term, v.LastLogIndex, v.LastLogTerm
	case *raft.InstallSnapshotRequest:
		q.Header, q.Term, q.Leader = encodeRaftHeader(v.RPCHeader), v.Term, v.Leader
		q.SnapshotVersion, q.LastLogIndex, q.LastLogTerm = int64(v.SnapshotVersion), v.LastLogIndex, v.LastLogTerm
		q.Peers, q.Configuration, q.ConfigurationIndex, q.Size = v.Peers, v.Configuration, v.ConfigurationIndex, v.Size
	case *raft.TimeoutNowRequest:
		q.Header = encodeRaftHeader(v.RPCHeader)
	default:
		return nil, fmt.Errorf("kv: unsupported raft request %T", value)
	}
	return q, nil
}

func decodeRaftRequest(method string, q *pb.RaftRequest) (any, error) {
	if q == nil {
		return nil, errors.New("kv: missing raft request")
	}
	h, err := decodeRaftHeader(q.Header)
	if err != nil {
		return nil, err
	}
	switch method {
	case "append":
		v := &raft.AppendEntriesRequest{RPCHeader: h, Term: q.Term, Leader: q.Leader, PrevLogEntry: q.PrevLogEntry, PrevLogTerm: q.PrevLogTerm, LeaderCommitIndex: q.LeaderCommitIndex}
		for _, entry := range q.Entries {
			if entry == nil || entry.Type > 255 {
				return nil, errors.New("kv: invalid raft log")
			}
			log := &raft.Log{Index: entry.Index, Term: entry.Term, Type: raft.LogType(entry.Type), Data: entry.Data, Extensions: entry.Extensions}
			if len(entry.AppendedAt) > 0 {
				if err := log.AppendedAt.UnmarshalBinary(entry.AppendedAt); err != nil {
					return nil, err
				}
			} else {
				log.AppendedAt = time.Time{}
			}
			v.Entries = append(v.Entries, log)
		}
		return v, nil
	case "vote":
		return &raft.RequestVoteRequest{RPCHeader: h, Term: q.Term, Candidate: q.Candidate, LastLogIndex: q.LastLogIndex, LastLogTerm: q.LastLogTerm, LeadershipTransfer: q.LeadershipTransfer}, nil
	case "prevote":
		return &raft.RequestPreVoteRequest{RPCHeader: h, Term: q.Term, LastLogIndex: q.LastLogIndex, LastLogTerm: q.LastLogTerm}, nil
	case "snapshot":
		version := raft.SnapshotVersion(q.SnapshotVersion)
		if int64(version) != q.SnapshotVersion || version < 0 || q.Size < 0 || q.Size > maxConsensusSnapshotBytes || int64(len(q.SnapshotData)) != q.Size {
			return nil, errors.New("kv: invalid raft snapshot metadata/body")
		}
		return &raft.InstallSnapshotRequest{RPCHeader: h, SnapshotVersion: version, Term: q.Term, Leader: q.Leader, LastLogIndex: q.LastLogIndex, LastLogTerm: q.LastLogTerm, Peers: q.Peers, Configuration: q.Configuration, ConfigurationIndex: q.ConfigurationIndex, Size: q.Size}, nil
	case "timeout":
		return &raft.TimeoutNowRequest{RPCHeader: h}, nil
	default:
		return nil, fmt.Errorf("kv: unknown consensus rpc %q", method)
	}
}

func encodeRaftResponse(value any) (*pb.RaftResponse, error) {
	q := &pb.RaftResponse{}
	switch v := value.(type) {
	case *raft.AppendEntriesResponse:
		q.Header, q.Term, q.LastLog, q.Success, q.NoRetryBackoff = encodeRaftHeader(v.RPCHeader), v.Term, v.LastLog, v.Success, v.NoRetryBackoff
	case *raft.RequestVoteResponse:
		q.Header, q.Term, q.Peers, q.Granted = encodeRaftHeader(v.RPCHeader), v.Term, v.Peers, v.Granted
	case *raft.RequestPreVoteResponse:
		q.Header, q.Term, q.Granted = encodeRaftHeader(v.RPCHeader), v.Term, v.Granted
	case *raft.InstallSnapshotResponse:
		q.Header, q.Term, q.Success = encodeRaftHeader(v.RPCHeader), v.Term, v.Success
	case *raft.TimeoutNowResponse:
		q.Header = encodeRaftHeader(v.RPCHeader)
	default:
		return nil, fmt.Errorf("kv: unsupported raft response %T", value)
	}
	return q, nil
}

func decodeRaftResponse(method string, q *pb.RaftResponse) (any, error) {
	if q == nil {
		return nil, errors.New("kv: missing raft response")
	}
	h, err := decodeRaftHeader(q.Header)
	if err != nil {
		return nil, err
	}
	switch method {
	case "append":
		return &raft.AppendEntriesResponse{RPCHeader: h, Term: q.Term, LastLog: q.LastLog, Success: q.Success, NoRetryBackoff: q.NoRetryBackoff}, nil
	case "vote":
		return &raft.RequestVoteResponse{RPCHeader: h, Term: q.Term, Peers: q.Peers, Granted: q.Granted}, nil
	case "prevote":
		return &raft.RequestPreVoteResponse{RPCHeader: h, Term: q.Term, Granted: q.Granted}, nil
	case "snapshot":
		return &raft.InstallSnapshotResponse{RPCHeader: h, Term: q.Term, Success: q.Success}, nil
	case "timeout":
		return &raft.TimeoutNowResponse{RPCHeader: h}, nil
	default:
		return nil, fmt.Errorf("kv: unknown consensus rpc %q", method)
	}
}
