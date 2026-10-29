package cluster

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// Count real encrypted records rather than assuming one net.Conn.Write is one
// packet. No key logging or TLS decryption hooks are needed.
type tlsRecordCountConn struct {
	net.Conn
	bytes   atomic.Int64
	records atomic.Int64
}

func (c *tlsRecordCountConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.bytes.Add(int64(n))
	for b := p[:n]; len(b) >= 5; {
		size := 5 + int(binary.BigEndian.Uint16(b[3:5]))
		if size > len(b) {
			break
		}
		if b[0] == 23 {
			c.records.Add(1)
		}
		b = b[size:]
	}
	return n, err
}

func tlsRecordTestPair(t testing.TB) (*tcpMTLSConn, *tcpMTLSConn, *tlsRecordCountConn) {
	t.Helper()
	ca := newTCPTestCA(t)
	a, err := NewTCPMTLSMeshTransportAdapter(ca.config(t, 1, nil)).loadTLS()
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewTCPMTLSMeshTransportAdapter(ca.config(t, 2, nil)).loadTLS()
	if err != nil {
		t.Fatal(err)
	}
	a.ServerName = "localhost"
	a.DynamicRecordSizingDisabled = true
	b.SessionTicketsDisabled = true
	left, right := net.Pipe()
	t.Cleanup(func() { _ = left.Close(); _ = right.Close() })
	wire := &tlsRecordCountConn{Conn: left}
	client, server := tls.Client(wire, a), tls.Server(right, b)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- server.HandshakeContext(ctx) }()
	if err := client.HandshakeContext(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	wire.bytes.Store(0)
	wire.records.Store(0)
	return &tcpMTLSConn{Conn: client, maxFrame: 1 << 20}, &tcpMTLSConn{Conn: server, maxFrame: 1 << 20}, wire
}

func TestTCPMTLSSmallFramesUseOneTLSRecord(t *testing.T) {
	sender, receiver, wire := tlsRecordTestPair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, size := range []int{1, 256, 1400, 16*1024 - 4, 16*1024 + 1, 256} {
		payload := bytes.Repeat([]byte{byte(size)}, size)
		before := wire.records.Load()
		done := make(chan error, 1)
		go func() { done <- sender.Send(ctx, payload) }()
		// Receive uses the unchanged length-prefixed reader, verifying that
		// combining TLS writes does not change framing for existing nodes.
		got, err := receiver.Receive(ctx)
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatalf("frame size=%d mismatch: %v", size, err)
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if size+4 <= 16*1024 && wire.records.Load()-before != 1 {
			t.Errorf("frame size=%d used %d TLS records, want 1", size, wire.records.Load()-before)
		}
		if cap(sender.sendBuf) > tcpMTLSCoalesceFrameBytes {
			t.Fatalf("large frame retained an unbounded send buffer: %d", cap(sender.sendBuf))
		}
	}
}

func TestTCPMTLSConcurrentCoalescedFrames(t *testing.T) {
	sender, receiver, wire := tlsRecordTestPair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const frames = 32
	done := make(chan error, frames)
	for i := 0; i < frames; i++ {
		go func(i int) {
			done <- sender.Send(ctx, bytes.Repeat([]byte{byte(i)}, 128+i))
		}(i)
	}
	seen := make(map[byte]bool)
	for i := 0; i < frames; i++ {
		got, err := receiver.Receive(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) == 0 || seen[got[0]] || !bytes.Equal(got, bytes.Repeat(got[:1], 128+int(got[0]))) {
			t.Fatalf("interleaved or duplicated frame: %x", got)
		}
		seen[got[0]] = true
	}
	for i := 0; i < frames; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if wire.records.Load() != frames {
		t.Fatalf("TLS records=%d want=%d", wire.records.Load(), frames)
	}
}

type fragmentedFrameWriteConn struct {
	net.Conn
	maxWrite  int
	failAfter int
	writes    int
	zero      bool
	closed    atomic.Bool
}

func (c *fragmentedFrameWriteConn) Write(p []byte) (int, error) {
	if c.zero {
		return 0, nil
	}
	if c.failAfter > 0 && c.writes >= c.failAfter {
		return 0, io.ErrClosedPipe
	}
	c.writes++
	if c.maxWrite > 0 && len(p) > c.maxWrite {
		p = p[:c.maxWrite]
	}
	return c.Conn.Write(p)
}

func (c *fragmentedFrameWriteConn) Close() error {
	c.closed.Store(true)
	return c.Conn.Close()
}

func TestTCPMTLSCoalescedFrameShortWrites(t *testing.T) {
	for _, mode := range []string{"short", "partial-failure", "zero-progress"} {
		t.Run(mode, func(t *testing.T) {
			left, right := net.Pipe()
			defer left.Close()
			defer right.Close()
			raw := &fragmentedFrameWriteConn{Conn: left, maxWrite: 3}
			if mode == "partial-failure" {
				raw.failAfter = 1
			}
			raw.zero = mode == "zero-progress"
			sender := &tcpMTLSConn{Conn: raw, maxFrame: 1024}
			receiver := &tcpMTLSConn{Conn: right, maxFrame: 1024}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			payload := bytes.Repeat([]byte{7}, 128)
			done := make(chan error, 1)
			go func() { done <- sender.Send(ctx, payload) }()
			got, receiveErr := receiver.Receive(ctx)
			sendErr := <-done
			if mode == "short" {
				if sendErr != nil || receiveErr != nil || !bytes.Equal(got, payload) {
					t.Fatalf("short writes corrupted frame: send=%v receive=%v", sendErr, receiveErr)
				}
			} else if sendErr == nil || receiveErr == nil || !raw.closed.Load() {
				t.Fatalf("partial frame must retire connection: send=%v receive=%v closed=%v", sendErr, receiveErr, raw.closed.Load())
			}
		})
	}
}

func BenchmarkTCPMTLSFrameSend(b *testing.B) {
	for _, size := range []int{64, 256, 1400, 4096, 32768} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			sender, receiver, wire := tlsRecordTestPair(b)
			payload := bytes.Repeat([]byte{1}, size)
			done := make(chan error, 1)
			go func() {
				_, err := io.CopyN(io.Discard, receiver.Conn, int64(b.N)*int64(size+4))
				done <- err
			}()
			b.ReportAllocs()
			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := sender.Send(context.Background(), payload); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			if err := <-done; err != nil {
				b.Fatal(err)
			}
			b.ReportMetric(float64(wire.bytes.Load())/float64(b.N), "TLS-B/op")
			b.ReportMetric(float64(wire.records.Load())/float64(b.N), "TLS-records/op")
		})
	}
}
