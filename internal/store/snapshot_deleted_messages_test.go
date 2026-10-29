package store

import (
	"bytes"
	"context"
	"fmt"
	"testing"
)

func TestSnapshotDeletedRecipientMessagesConvergeAcrossBackends(t *testing.T) {
	for _, sourceBackend := range []string{"sqlite", "pebble"} {
		for _, targetBackend := range []string{"sqlite", "pebble"} {
			t.Run(sourceBackend+"-to-"+targetBackend, func(t *testing.T) {
				open := func(name, backend string, slot uint16) *Store {
					if backend == "pebble" {
						return openPebbleTestStore(t, name, slot, 2)
					}
					return openNamedTestStoreWithWindow(t, name, slot, 2)
				}
				source := open("source", sourceBackend, 1)
				if sourceBackend == "sqlite" {
					defer source.Close()
				}
				target := open("target", targetBackend, 2)
				if targetBackend == "sqlite" {
					defer target.Close()
				}
				ctx := context.Background()
				user, _, err := source.CreateUser(ctx, CreateUserParams{Username: "deleted-recipient", PasswordHash: "test"})
				if err != nil {
					t.Fatal(err)
				}
				for i := 0; i < 3; i++ {
					if _, _, err := source.CreateMessage(ctx, CreateMessageParams{UserKey: user.Key(), Sender: testSenderKey(9, 1), Body: []byte(fmt.Sprint(i))}); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := source.DeleteUser(ctx, user.Key()); err != nil {
					t.Fatal(err)
				}
				partition := MessageSnapshotPartition(source.NodeID())
				messages, err := source.BuildSnapshotChunk(ctx, partition)
				if err != nil {
					t.Fatal(err)
				}
				if len(messages.Rows) != 2 {
					t.Fatalf("source history must retain the local window: %d", len(messages.Rows))
				}
				// An unknown recipient must still be skipped. Importing historical
				// messages requires its user row to have arrived via users/full.
				if err := target.ApplySnapshotChunk(ctx, messages); err != nil {
					t.Fatal(err)
				}
				beforeUsers, err := target.BuildSnapshotChunk(ctx, partition)
				if err != nil || len(beforeUsers.Rows) != 0 {
					t.Fatalf("unknown recipient was imported: rows=%v err=%v", beforeUsers, err)
				}
				users, err := source.BuildSnapshotChunk(ctx, SnapshotUsersPartition)
				if err != nil {
					t.Fatal(err)
				}
				if err := target.ApplySnapshotChunk(ctx, users); err != nil {
					t.Fatal(err)
				}
				for i := 0; i < 2; i++ {
					if err := target.ApplySnapshotChunk(ctx, messages); err != nil {
						t.Fatal(err)
					}
					actual, err := target.BuildSnapshotChunk(ctx, partition)
					if err != nil {
						t.Fatal(err)
					}
					wantHash, err := hashSnapshotRows(messages.Rows)
					if err != nil {
						t.Fatal(err)
					}
					gotHash, err := hashSnapshotRows(actual.Rows)
					if err != nil {
						t.Fatal(err)
					}
					if len(actual.Rows) != len(messages.Rows) || !bytes.Equal(wantHash, gotHash) {
						t.Fatalf("deleted recipient message snapshot diverged: source=%d target=%d", len(messages.Rows), len(actual.Rows))
					}
				}
				assertUsersSnapshotEqual(t, source, target)
				if _, err := target.GetUser(ctx, user.Key()); err != ErrNotFound {
					t.Fatalf("deleted user became visible: %v", err)
				}
				if _, err := target.ListMessagesByUser(ctx, user.Key(), 10); err != ErrNotFound {
					t.Fatalf("deleted user's messages became readable: %v", err)
				}
			})
		}
	}
}
