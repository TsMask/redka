package store

import (
	"context"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestExclusiveTransactionWaitsForWriteCommitter(t *testing.T) {
	db, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer db.Close()

	batchStarted := make(chan struct{})
	releaseBatch := make(chan struct{})
	writeDone := make(chan error, 1)
	write := func() {
		writeDone <- db.TransactionWriteItems(context.Background(), "barrier", true,
			func(_ *gorm.DB, _ Dialect, _ []any) error {
				close(batchStarted)
				<-releaseBatch
				return nil
			})
	}
	go write()

	select {
	case <-batchStarted:
	case <-time.After(time.Second):
		t.Fatal("write batch did not start")
	}

	exclusiveDone := make(chan error, 1)
	exclusive := func() {
		exclusiveDone <- db.ExclusiveTransaction(context.Background(),
			func(_ *gorm.DB, _ Dialect) error { return nil })
	}
	go exclusive()

	select {
	case err := <-exclusiveDone:
		t.Fatalf("exclusive transaction completed while write was active: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	close(releaseBatch)
	if err := <-writeDone; err != nil {
		t.Fatalf("write batch: %v", err)
	}

	select {
	case err := <-exclusiveDone:
		if err != nil {
			t.Fatalf("exclusive transaction: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("exclusive transaction did not finish after write batch")
	}
}
