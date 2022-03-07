package bitcoin_reader

import (
	"testing"
	"time"

	"github.com/tokenized/bitcoin_reader/internal/platform/tests"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/pkg/threads"
)

func Test_NodeManager(t *testing.T) {
	if !testing.Verbose() {
		t.Skip() // Don't want to redownload the block all the time
	}

	ctx := tests.Context()
	store := storage.NewMockStorage()
	peers := NewPeerRepository(store, "")
	headers := NewMockHeaderRepository()
	config := &Config{
		Network:          bitcoin.MainNet,
		DesiredNodeCount: 20,
	}

	peers.LoadSeeds(ctx, bitcoin.MainNet)

	manager := NewNodeManager("/Tokenized/Spynode:Test/", config, headers, peers)
	runThread := threads.NewThread("Run", manager.Run)
	runComplete := runThread.GetCompleteChannel()
	runThread.Start(ctx)

	select {
	case <-runComplete:
		if err := runThread.Error(); err != nil {
			t.Errorf("Failed to run : %s", err)
		}
		t.Errorf("Completed without interrupt")

	case <-time.After(20 * time.Second):
		t.Logf("Shutting down")
		runThread.Stop(ctx)
		select {
		case <-runComplete:
			if err := runThread.Error(); err != nil {
				t.Errorf("Failed to run : %s", err)
			}

		case <-time.After(time.Second):
			t.Fatalf("Failed to shut down")
		}
	}
}
