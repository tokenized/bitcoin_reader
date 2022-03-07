package bitcoin_reader

import (
	"context"
	"testing"

	"github.com/tokenized/bitcoin_reader/internal/platform/tests"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/storage"
)

func Test_LoadSeeds(t *testing.T) {
	ctx := tests.Context()
	store := storage.NewMockStorage()
	peers := NewPeerRepository(store, "")

	peers.LoadSeeds(ctx, bitcoin.MainNet)
}

func TestPeers(t *testing.T) {
	addresses := []string{
		"test 1",
		"test 2",
		"test 3",
		"test 4",
		"test 5",
		"test 6",
	}

	ctx := tests.Context()
	store := storage.NewMockStorage()
	repo := NewPeerRepository(store, "")

	// For logging to test from within functions
	ctx = context.WithValue(ctx, 999, t)
	// Use this to get the test value from within non-test code.
	// testValue := ctx.Value(999)
	// test, ok := testValue.(*testing.T)
	// if ok {
	// test.Logf("Test Debug Message")
	// }

	repo.Clear(ctx)

	// Load
	if err := repo.Load(ctx); err != nil {
		t.Errorf("Failed to load repo : %v", err)
	}

	// Add
	for _, address := range addresses {
		added, err := repo.Add(ctx, address)
		if err != nil {
			t.Errorf("Failed to add address : %v", err)
		}
		if !added {
			t.Errorf("Didn't add address : %s", address)
		}
	}

	// Get min score 0
	peers, err := repo.Get(ctx, 0, -1)
	if err != nil {
		t.Errorf("Failed to get addresses : %v", err)
	}

	for _, address := range addresses {
		found := false
		for _, peer := range peers {
			if peer.Address == address {
				t.Logf("Found address : %s", address)
				found = true
				break
			}
		}

		if !found {
			t.Errorf("Failed to find address : %s", address)
		}
	}

	// Get min score 0
	peers, err = repo.Get(ctx, 1, -1)
	if err != nil {
		t.Errorf("Failed to get addresses : %v", err)
	}

	if len(peers) > 0 {
		t.Errorf("Pulled high score peers")
	}

	// Save
	t.Logf("Saving")
	if err := repo.Save(ctx); err != nil {
		t.Errorf("Failed to save repo : %v", err)
	}

	// Load
	t.Logf("Reloading")
	if err := repo.Load(ctx); err != nil {
		t.Errorf("Failed to re-load repo : %v", err)
	}

	// Get min score 0
	peers, err = repo.Get(ctx, 0, -1)
	if err != nil {
		t.Errorf("Failed to get addresses : %v", err)
	}

	for _, address := range addresses {
		found := false
		for _, peer := range peers {
			if peer.Address == address {
				t.Logf("Found address : %s", address)
				found = true
				break
			}
		}

		if !found {
			t.Errorf("Failed to find address : %s", address)
		}
	}

	// Get min score 0
	peers, err = repo.Get(ctx, 1, -1)
	if err != nil {
		t.Errorf("Failed to get addresses : %v", err)
	}

	if len(peers) > 0 {
		t.Errorf("Pulled high score peers")
	}
}
