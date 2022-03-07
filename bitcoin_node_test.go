package bitcoin_reader

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/tokenized/bitcoin_reader/internal/platform/tests"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/pkg/threads"
)

func Test_Handshake(t *testing.T) {
	if !testing.Verbose() {
		t.Skip() // Don't want to redownload the block all the time
	}
	ctx := tests.Context()
	store := storage.NewMockStorage()

	config := &Config{
		Network: bitcoin.MainNet,
	}

	headers := NewMockHeaderRepository()
	peers := NewPeerRepository(store, "")

	address := "bitcoind.shared.tokenized.com:8333"

	node := NewBitcoinNode(address, "/Tokenized/Spynode:Test/", config, headers, peers)
	node.SetVerifyOnly()

	runThread := threads.NewThread("Run", node.Run)
	runComplete := runThread.GetCompleteChannel()
	runThread.Start(ctx)

	select {
	case <-runComplete:
		if err := runThread.Error(); err != nil {
			t.Errorf("Failed to run : %s", err)
		}

	case <-time.After(3 * time.Second):
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

	if !node.Verified() {
		t.Errorf("Failed to verify node")
	}
}

func Test_FindPeers(t *testing.T) {
	if !testing.Verbose() {
		t.Skip() // Don't want to redownload the block all the time
	}
	ctx := tests.Context()
	store := storage.NewMockStorage()

	config := &Config{
		Network: bitcoin.MainNet,
	}

	headers := NewMockHeaderRepository()
	peers := NewPeerRepository(store, "")
	// if err := peers.Load(ctx); err != nil {
	// 	t.Fatalf("Failed to load peers : %s", err)
	// }
	peers.LoadSeeds(ctx, config.Network)

	peerList, err := peers.Get(ctx, 0, -1)
	if err != nil {
		t.Fatalf("Failed to get peers : %s", err)
	}

	if len(peerList) == 0 {
		t.Fatalf("No peers returned")
	}

	var wait sync.WaitGroup
	var nodes []*BitcoinNode
	var threadList threads.Threads
	for i, peer := range peerList {
		node := NewBitcoinNode(peer.Address, "/Tokenized/Spynode:Test/", config, headers, peers)
		node.SetVerifyOnly()
		nodes = append(nodes, node)

		thread := threads.NewThread(fmt.Sprintf("Run (%d)", i), node.Run)
		thread.SetWait(&wait)
		threadList = append(threadList, thread)
		thread.Start(ctx)

		if i%100 == 0 {
			time.Sleep(5 * time.Second)
		}

		// if i >= 100 {
		// 	break
		// }
	}

	time.Sleep(5 * time.Second)
	t.Logf("Stopping")
	threadList.Stop(ctx)
	wait.Wait()

	verifiedCount := 0
	for _, node := range nodes {
		if node.Verified() {
			verifiedCount++

			// ip := node.IP.To16()
			// value := "\t{[]byte{"
			// for i, b := range ip {
			// 	if i != 0 {
			// 		value += ", "
			// 	}
			// 	value += fmt.Sprintf("0x%02x", b)
			// }
			// value += fmt.Sprintf("}, %d},", node.Port)

			// fmt.Printf("%s\n", value)
		}
	}

	t.Logf("Verified count : %d/%d", verifiedCount, len(nodes))
}
