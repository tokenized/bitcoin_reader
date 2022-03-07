package bitcoin_reader

import (
	"bytes"
	"math/rand"
	"testing"
	"time"

	"github.com/tokenized/bitcoin_reader/internal/platform/tests"
	"github.com/tokenized/pkg/bitcoin"

	"github.com/google/uuid"
)

func Test_TxManager_General(t *testing.T) {
	ctx := tests.Context()
	manager := NewTxManager(250 * time.Millisecond)
	node1 := uuid.New()
	node2 := uuid.New()

	txids := make([]bitcoin.Hash32, 10)
	for i := range txids {
		rand.Read(txids[i][:])
	}

	needsRequest, err := manager.AddTxID(ctx, node1, txids[0])
	if err != nil {
		t.Fatalf("Failed to add txid : %s", err)
	}
	if !needsRequest {
		t.Errorf("Tx should need requested")
	}

	needsRequest, err = manager.AddTxID(ctx, node2, txids[0])
	if err != nil {
		t.Fatalf("Failed to add txid : %s", err)
	}
	if needsRequest {
		t.Errorf("Tx should not need requested")
	}

	time.Sleep(300 * time.Millisecond)

	requestTxIDs, err := manager.GetTxRequests(ctx, node2, 100)
	if err != nil {
		t.Fatalf("Failed to get tx requests : %s", err)
	}

	if len(requestTxIDs) != 1 {
		t.Fatalf("Wrong request count : got %d, want %d", len(requestTxIDs), 1)
	}
	if !bytes.Equal(requestTxIDs[0][:], txids[0][:]) {
		t.Errorf("Wrong request txid : \ngot  : %s\nwant : %s", requestTxIDs[0], txids[0])
	}
}
