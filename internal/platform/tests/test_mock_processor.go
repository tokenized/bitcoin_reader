package tests

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/merkle_proof"
	"github.com/tokenized/pkg/miner_id"
	"github.com/tokenized/pkg/wire"

	"github.com/pkg/errors"
)

type MockDataProcessor struct {
	rand        *rand.Rand
	relevantTxs map[bitcoin.Hash32]bool

	sync.Mutex
}

func NewMockDataProcessor() *MockDataProcessor {
	return &MockDataProcessor{
		rand:        rand.New(rand.NewSource(time.Now().UnixNano())),
		relevantTxs: make(map[bitcoin.Hash32]bool),
	}
}

func (m *MockDataProcessor) randomRelavent() bool {
	return m.rand.Intn(1000) == 1
}

func (m *MockDataProcessor) ProcessTx(ctx context.Context, tx *wire.MsgTx) (bool, error) {
	m.Lock()
	defer m.Unlock()

	txid := *tx.TxHash()
	_, isRelevant := m.relevantTxs[txid]
	if !isRelevant {
		isRelevant = m.randomRelavent()
	}

	return isRelevant, nil
}

func (m *MockDataProcessor) CancelTx(ctx context.Context, txid bitcoin.Hash32) error {
	m.Lock()
	defer m.Unlock()

	return nil
}

func (m *MockDataProcessor) AddTxConflict(ctx context.Context,
	txid, conflictTxID bitcoin.Hash32) error {
	m.Lock()
	defer m.Unlock()

	return nil
}

func (m *MockDataProcessor) ConfirmTx(ctx context.Context, txid bitcoin.Hash32, blockHeight int,
	merkleProof *merkle_proof.MerkleProof) error {
	m.Lock()
	defer m.Unlock()

	// js, _ := json.MarshalIndent(merkleProof, "", "  ")
	// fmt.Printf("Confirmed tx : %s\n%s\n", txid, js)
	delete(m.relevantTxs, txid)

	return nil
}

func (m *MockDataProcessor) UpdateTxChainDepth(ctx context.Context, txid bitcoin.Hash32,
	chainDepth uint32) error {
	m.Lock()
	defer m.Unlock()

	return nil
}

func (m *MockDataProcessor) ProcessCoinbaseTx(ctx context.Context, blockHash bitcoin.Hash32,
	tx *wire.MsgTx) error {
	m.Lock()
	defer m.Unlock()

	for i, output := range tx.TxOut {
		minerID, err := miner_id.ParseMinerIDFromScript(output.LockingScript)
		if err != nil {
			if errors.Cause(err) != miner_id.ErrNotMinerID {
				logger.WarnWithFields(ctx, []logger.Field{
					logger.Stringer("txid", tx.TxHash()),
					logger.Int("output", i),
				}, "Failed to parse miner id output script : %s", err)
			}
			continue
		}

		logger.InfoWithFields(ctx, []logger.Field{
			logger.JSON("miner_id", minerID),
		}, "Found miner id")

		break
	}

	return nil
}
