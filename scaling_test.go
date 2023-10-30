package bitcoin_reader

import (
	"context"
	"fmt"
	mathRand "math/rand"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tokenized/bitcoin_reader/headers"
	"github.com/tokenized/config"
	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/merkle_proof"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/threads"

	"github.com/pkg/errors"
)

func Test_Memory(t *testing.T) {
	t.Skip("With processor count at 4 or more it doesn't seem to accumulate memory")

	Run_Test_Memory(t, 2, 50, 1000000, 10, 10, time.Microsecond*10)
}

// nodeCount is how many mock node connections to use.
// totalTxCount is the total number of transactions to generate and distribute.
// distCount is how many nodes to send each tx to.
// checkCount is how many times to pull memory stats.
func Run_Test_Memory(t *testing.T, cpuCount, nodeCount, totalTxCount, distCount, checkCount int,
	txFrequency time.Duration) {

	ctx := logger.ContextWithLogger(context.Background(), true, false, "")
	store := storage.NewMockStorage()

	headersRepo := headers.NewRepository(headers.DefaultConfig(), store)
	startTime := uint32(952644136)
	headersRepo.DisableDifficulty()
	headersRepo.InitializeWithTimeStamp(startTime)
	headers.MockHeaders(ctx, headersRepo, headersRepo.LastHash(), startTime, 1100)
	if err := headersRepo.Clean(ctx); err != nil {
		t.Fatalf("Failed to clean headers : %s", err)
	}

	peers := NewPeerRepository(store, "")

	txManager := NewTxManager(time.Second * 10)
	txProcessor := NewMockTxProcessor()
	txManager.SetTxProcessor(txProcessor)

	nodeConfig := &Config{
		Network:                 bitcoin.MainNet,
		Timeout:                 config.NewDuration(time.Hour * 4),
		ScanCount:               1000,
		TxRequestCount:          10000,
		StartupDelay:            config.NewDuration(time.Minute),
		ConcurrentBlockRequests: 2,
		DesiredNodeCount:        50,
		StartBlockHeight:        700000,
		BlockRequestDelay:       config.NewDuration(time.Second * 5),
	}
	nodes := make([]*MockNode, nodeCount)
	for i := range nodes {
		nodes[i] = NewMockNode(fmt.Sprintf("Node %d", i), nodeConfig, headersRepo, peers, txManager)
	}

	txs := GenerateMockTxs(totalTxCount, runtime.GOMAXPROCS(0)/2)

	trailingTxCount := 1000
	txs2 := GenerateMockTxs(trailingTxCount, 1)

	// Start all the nodes.
	var wait sync.WaitGroup
	var stopper threads.StopCombiner
	selects := make([]reflect.SelectCase, nodeCount+2)
	nodeThreads := make([]*threads.InterruptableThread, nodeCount)
	for i, node := range nodes {
		thread, complete := threads.NewInterruptableThreadComplete(fmt.Sprintf("Node %d", i),
			node.Run, &wait)
		selects[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(complete),
		}
		nodeThreads[i] = thread
		stopper.Add(thread)
	}

	distributeInterrupt := make(chan interface{})
	distributeComplete := make(chan error)
	selects[nodeCount] = reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(distributeComplete),
	}

	txManagerComplete := make(chan error, 1)
	selects[nodeCount+1] = reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(txManagerComplete),
	}

	runtime.GC()
	time.Sleep(time.Second)
	runtime.GC()
	time.Sleep(time.Second)
	runtime.GC()
	time.Sleep(time.Second)
	runtime.GC()

	t.Logf("Set GOMAXPROCS=%d, previously %d", cpuCount, runtime.GOMAXPROCS(cpuCount))

	wait.Add(1)
	go func() {
		txManagerComplete <- txManager.Run(ctx)
		wait.Done()
	}()

	for _, thread := range nodeThreads {
		thread.Start(ctx)
	}
	time.Sleep(time.Second) // give nodes time to shake hands

	wait.Add(1)
	go func() {
		var priorMemStats runtime.MemStats
		runtime.ReadMemStats(&priorMemStats)
		t.Logf("Prior Mem Stats : %s", formatMemStats(priorMemStats))

		txChunkCount := totalTxCount / checkCount
		for i := 0; i < checkCount; i++ {
			if txChunkCount >= len(txs) {
				txChunkCount = len(txs) - 1
			}
			if err := DistributeTxs(t, nodes, txs[:txChunkCount], distCount, txFrequency,
				distributeInterrupt); err != nil {
				distributeComplete <- err
				return
			}

			var memStats runtime.MemStats
			runtime.ReadMemStats(&memStats)
			t.Logf("Intermediate Mem Stats Diff : %s", formatMemStatsDiff(memStats, priorMemStats))
			txs = txs[txChunkCount:]
		}

		// Send a slow rate of trailing txs to see mem stats settle back down
		txChunkCount = trailingTxCount / checkCount
		for i := 0; i < checkCount; i++ {
			if txChunkCount >= len(txs2) {
				txChunkCount = len(txs2) - 1
			}
			if err := DistributeTxs(t, nodes, txs2[:txChunkCount], distCount, time.Millisecond*10,
				distributeInterrupt); err != nil {
				distributeComplete <- err
				return
			}

			var memStats runtime.MemStats
			runtime.ReadMemStats(&memStats)
			t.Logf("Trailing Mem Stats Diff : %s", formatMemStatsDiff(memStats, priorMemStats))
			txs2 = txs2[txChunkCount:]
		}

		wait.Done()
		distributeComplete <- nil
	}()

	waitComplete := make(chan interface{})
	go func() {
		wait.Wait()
		close(waitComplete)
	}()

	selectIndex, selectValue, valueReceived := reflect.Select(selects)
	var selectErr error
	if valueReceived {
		selectInterface := selectValue.Interface()
		if selectInterface != nil {
			err, ok := selectInterface.(error)
			if ok {
				selectErr = err
			}
		}
	}

	if selectIndex == len(nodes) {
		if selectErr != nil {
			t.Fatalf("Failed to distribute transactions : %s", selectErr)
		}
	} else if selectIndex == len(nodes)+1 {
		t.Fatalf("Failed to run tx manager : %s", selectErr)
	} else if selectErr != nil {
		t.Fatalf("Node %d failed : %s", selectIndex, selectErr)
	}

	t.Logf("Shutting down")
	close(distributeInterrupt)
	txManager.Stop(ctx)
	stopper.Stop(ctx) // Stop all nodes

	select {
	case <-waitComplete:
		t.Logf("Shutdown completed")
	case <-time.After(time.Second):
		t.Fatalf("Shutdown timed out")
	}

	t.Logf("GOMAXPROCS: %d", runtime.GOMAXPROCS(0))
}

func formatMemStats(memStats runtime.MemStats) string {
	return fmt.Sprintf("TotalAlloc: %0.6f, HeapAlloc %0.6f", float64(memStats.TotalAlloc)/1e6,
		float64(memStats.HeapAlloc)/1e6)
}

func formatMemStatsDiff(memStats, prior runtime.MemStats) string {
	return fmt.Sprintf("TotalAlloc: %0.6f, HeapAlloc %0.6f",
		(float64(memStats.TotalAlloc)-float64(prior.TotalAlloc))/1e6,
		(float64(memStats.HeapAlloc)-float64(prior.HeapAlloc))/1e6)
}

func GenerateMockTxs(totalTxCount, threadCount int) []*wire.MsgTx {
	var txCount uint64
	atomic.StoreUint64(&txCount, 0)
	complete := make(chan error, threadCount)
	result := make([]*wire.MsgTx, totalTxCount)
	offset := 0
	chunkCount := (totalTxCount / threadCount) + 1
	for i := 0; i < threadCount; i++ {
		if offset+chunkCount >= totalTxCount {
			chunkCount = totalTxCount - offset
		}
		set := result[offset : offset+chunkCount]
		go func() {
			complete <- generateMockTxSet(set, &txCount)
		}()
		offset += chunkCount
	}

	finishLogging := make(chan interface{})
	go func() {
		for {
			select {
			case <-time.After(time.Minute):
				currentCount := atomic.LoadUint64(&txCount)
				fmt.Printf("%d txs generated (%0.2f)", currentCount,
					(float64(currentCount)/float64(totalTxCount))*100.0)
			case <-finishLogging:
				return
			}
		}
	}()

	completeCount := 0
	for err := range complete {
		completeCount++
		if err != nil {
			panic(fmt.Sprintf("Failed to generate txs : %s", err))
		}
		if completeCount == threadCount {
			break
		}
	}

	close(finishLogging)
	fmt.Printf("%d txs generated", atomic.LoadUint64(&txCount))

	return result
}

func generateMockTxSet(txs []*wire.MsgTx, txCount *uint64) error {
	for i := range txs {
		tx := wire.NewMsgTx(0)

		inputCount := mathRand.Intn(5) + 1
		inputValue := uint64(0)
		for j := 0; j < inputCount; j++ {
			value := uint64(mathRand.Intn(10000) + 1)
			if j == 0 {
				value += 40
			}
			inputValue += value

			unlockingScript := make(bitcoin.Script, 34+73+1)
			mathRand.Read(unlockingScript[:])

			var randHash bitcoin.Hash32
			mathRand.Read(randHash[:])
			tx.AddTxIn(wire.NewTxIn(wire.NewOutPoint(&randHash, uint32(mathRand.Intn(5))),
				unlockingScript))
		}

		inputValue -= 20 // tx fee
		for inputValue > 0 {
			value := uint64(mathRand.Intn(10000) + 1)
			if value > inputValue {
				value = inputValue
			}
			inputValue -= value

			key, _ := bitcoin.GenerateKey(bitcoin.MainNet)
			lockingScript, _ := key.LockingScript()
			tx.AddTxOut(wire.NewTxOut(value, lockingScript))
		}

		txs[i] = tx

		atomic.AddUint64(txCount, 1)
	}

	return nil
}

func DistributeTxs(t *testing.T, nodes []*MockNode, txs []*wire.MsgTx, distCount int,
	txFrequency time.Duration, interrupt <-chan interface{}) error {

	start := time.Now()

	l := len(nodes)
	for _, tx := range txs {
		// t.Logf("Sending tx %d: %s", i, tx.TxHash())

		// Pick some nodes and deliver tx
		count := mathRand.Intn(distCount) + 1
		for i := 0; i < count; i++ {
			nodeIndex := mathRand.Intn(l)
			if err := nodes[nodeIndex].ExternalNode.ProvideTx(tx); err != nil {
				return errors.Wrapf(err, "provide tx: %d", i)
			}
		}

		if txFrequency == 0 {
			select {
			default:
			case <-interrupt:
				return errors.New("Distribute interrupted")
			}
		} else {
			select {
			case <-time.After(txFrequency): // wait to deliver next tx
			case <-interrupt:
				return errors.New("Distribute interrupted")
			}
		}
	}

	elapsed := time.Since(start).Seconds()

	t.Logf("Finished distributing %d txs in %0.4f seconds (%0.4f/sec)", len(txs), elapsed,
		float64(len(txs))/elapsed)
	return nil
}

type MockTxProcessor struct {
}

func NewMockTxProcessor() *MockTxProcessor {
	return &MockTxProcessor{}
}

// ProcessTx returns true if the tx is relevant.
func (p *MockTxProcessor) ProcessTx(ctx context.Context, tx *wire.MsgTx) (bool, error) {
	// time.Sleep(time.Microsecond * 10)
	for i := 0; i < 100000; i++ {

	}
	return false, nil
}

// CancelTx specifies that a tx is no longer valid because a conflicting tx has been confirmed.
func (p *MockTxProcessor) CancelTx(ctx context.Context, txid bitcoin.Hash32) error {
	return nil
}

// AddTxConflict specifies that there is an unconfirmed conflicting tx to a relevant tx.
func (p *MockTxProcessor) AddTxConflict(ctx context.Context,
	txid, conflictTxID bitcoin.Hash32) error {
	return nil
}

func (p *MockTxProcessor) ConfirmTx(ctx context.Context, txid bitcoin.Hash32, blockHeight int,
	merkleProof *merkle_proof.MerkleProof) error {
	return nil
}

func (p *MockTxProcessor) UpdateTxChainDepth(ctx context.Context, txid bitcoin.Hash32,
	chainDepth uint32) error {
	return nil
}

func (p *MockTxProcessor) ProcessCoinbaseTx(ctx context.Context, blockHash bitcoin.Hash32,
	tx *wire.MsgTx) error {
	return nil
}
