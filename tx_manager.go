package bitcoin_reader

import (
	"bytes"
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/wire"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

// TxManager manages tx inventories and determines when txs should be requested from peer nodes.
// It creates 256 txid maps and uses the first byte of the txid to divide the txs. This allows
// separate locks on each map and reduces the size of each map.
type TxManager struct {
	txMaps         []*txMap
	requestTimeout time.Duration

	txChannel       chan *wire.MsgTx
	txChannelClosed bool
	txChannelLock   sync.Mutex

	txProcessor TxProcessor
	txSaver     TxSaver

	sync.RWMutex
}

type txMap struct {
	txs map[bitcoin.Hash32]*TxData

	sync.RWMutex
}

type TxData struct {
	FirstSeen     time.Time   // time the txid was first seen in an inventory message
	LastRequested time.Time   // time of the last request for the full tx
	Received      *time.Time  // time the full tx was first seen. nil when not seen yet
	NodeIDs       []uuid.UUID // the nodes that have announced this tx. not including nodes the tx has been requested from
	ReceivedFrom  *uuid.UUID  // the node that provided the full tx. possibly use to track bad nodes

	sync.RWMutex
}

func NewTxManager(requestTimeout time.Duration) *TxManager {
	result := &TxManager{
		txMaps:         make([]*txMap, 256),
		requestTimeout: requestTimeout,
	}

	for i := range result.txMaps {
		result.txMaps[i] = newTxMap()
	}

	return result
}

func (m *TxManager) SetTxProcessor(txProcessor TxProcessor) {
	m.Lock()
	m.txProcessor = txProcessor

	if txProcessor != nil {
		m.txChannel = make(chan *wire.MsgTx, 1000)
	}
	m.Unlock()
}

func (m *TxManager) SetTxSaver(txSaver TxSaver) {
	m.Lock()
	m.txSaver = txSaver
	m.Unlock()
}

func (m *TxManager) Run(ctx context.Context, interrupt <-chan interface{}) error {
	if m.txChannel == nil {
		select {
		case <-interrupt:
			return nil
		}
	}

	m.Lock()
	txProcessor := m.txProcessor
	txSaver := m.txSaver
	m.Unlock()

	for {
		select {
		case <-interrupt:
			for range m.txChannel { // flush channel
			}
			return nil

		case tx, ok := <-m.txChannel:
			if !ok { // channel closed
				return nil
			}

			if txProcessor != nil {
				isRelevant, err := txProcessor.ProcessTx(ctx, tx)
				if err != nil {
					return errors.Wrap(err, "process tx")
				}

				if isRelevant {
					logger.InfoWithFields(ctx, []logger.Field{
						logger.Stringer("txid", tx.TxHash()),
						logger.Float64("tx_size_kb", float64(tx.SerializeSize())/1e3),
					}, "Relevant Tx")

					if txSaver != nil {
						if err := txSaver.SaveTx(ctx, tx); err != nil {
							return errors.Wrap(err, "save tx")
						}
					}
				}
			}
		}
	}
}

func (m *TxManager) Stop(ctx context.Context) {
	m.txChannelLock.Lock()
	if m.txChannel != nil {
		close(m.txChannel)
		m.txChannelClosed = true
	}
	m.txChannelLock.Unlock()
}

func newTxMap() *txMap {
	return &txMap{
		txs: make(map[bitcoin.Hash32]*TxData),
	}
}

// AddTxID adds a txid to the tx manager and returns true if the tx should be requested.
func (m *TxManager) AddTxID(ctx context.Context, nodeID uuid.UUID,
	txid bitcoin.Hash32) (bool, error) {

	m.RLock()
	txMap := m.txMaps[txid[0]]
	requestTimeout := m.requestTimeout
	m.RUnlock()

	txMap.Lock()
	data, exists := txMap.txs[txid]
	if exists {
		txMap.Unlock()
		data.Lock()

		if data.Received != nil {
			data.Unlock()
			return false, nil // already received tx
		}

		if time.Since(data.LastRequested) < requestTimeout {
			// Save node id to request later if this request doesn't return
			data.NodeIDs = appendID(data.NodeIDs, nodeID)
			data.Unlock()
			return false, nil // requested recently
		}

		// Request timed out, so request again
		data.LastRequested = time.Now()

		// Make sure the node id isn't in the list since it is being requested
		data.NodeIDs = removeID(data.NodeIDs, nodeID)

		data.Unlock()
		return true, nil
	}

	now := time.Now()
	data = &TxData{
		FirstSeen:     now,
		LastRequested: now,
		NodeIDs:       []uuid.UUID{}, // don't include this node id since it will be requested now
	}

	txMap.txs[txid] = data
	txMap.Unlock()

	return true, nil
}

func removeID(ids []uuid.UUID, newID uuid.UUID) []uuid.UUID {
	for i, id := range ids {
		if bytes.Equal(id[:], newID[:]) {
			return append(ids[:i], ids[i+1:]...)
		}
	}

	return ids // not in list
}

func appendID(ids []uuid.UUID, newID uuid.UUID) []uuid.UUID {
	for _, id := range ids {
		if bytes.Equal(id[:], newID[:]) {
			return ids // already in list
		}
	}

	return append(ids, newID) // append to list
}

// AddTx registers that we have received the full tx data. We want to keep the txid around for a
// while to ensure we don't re-request it in case it is still propagating some of the nodes.
func (m *TxManager) AddTx(ctx context.Context, nodeID uuid.UUID, tx *wire.MsgTx) error {
	now := time.Now()
	txid := *tx.TxHash()

	m.RLock()
	txMap := m.txMaps[txid[0]]
	m.RUnlock()

	txMap.Lock()
	data, exists := txMap.txs[txid]
	if exists {
		txMap.Unlock()

		isNew := false
		data.Lock()
		if data.Received == nil {
			data.Received = &now
			data.ReceivedFrom = &nodeID
			isNew = true
		}
		data.Unlock()

		if isNew {
			m.sendTx(tx)
		}

		return nil
	}

	// Add new item
	data = &TxData{
		FirstSeen:     now,
		LastRequested: now,
		Received:      &now,
		ReceivedFrom:  &nodeID,
	}

	txMap.txs[txid] = data
	txMap.Unlock()

	m.sendTx(tx)
	return nil
}

// sendTx adds a tx to the tx channel if it is set.
func (m *TxManager) sendTx(tx *wire.MsgTx) {
	m.txChannelLock.Lock()
	defer m.txChannelLock.Unlock()

	if m.txChannelClosed {
		return
	}

	m.txChannel <- tx
}

type indexList []int

func (l indexList) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

// GetTxRequests returns a set of txs that need to be requested again.
func (m *TxManager) GetTxRequests(ctx context.Context, nodeID uuid.UUID,
	max int) ([]bitcoin.Hash32, error) {

	m.RLock()
	requestTimeout := m.requestTimeout
	m.RUnlock()

	// Randomly sort indexes
	indexes := make(indexList, 256)
	for i := range indexes {
		indexes[i] = i
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(indexes), indexes.Swap)

	now := time.Now()
	var result []bitcoin.Hash32
	count := 0
	for _, index := range indexes {
		m.RLock()
		txMap := m.txMaps[index]
		m.RUnlock()

		txMap.RLock()
		for txid, data := range txMap.txs {
			data.Lock()
			if data.Received != nil {
				data.Unlock()
				continue // already received
			}

			if !contains(data.NodeIDs, nodeID) {
				data.Unlock()
				continue // not reported by this node
			}

			if time.Since(data.LastRequested) < requestTimeout {
				data.Unlock()
				continue // recently requested
			}

			data.LastRequested = now
			// Remove node id from the list since it is being requested
			data.NodeIDs = removeID(data.NodeIDs, nodeID)
			data.Unlock()

			result = append(result, txid)
			count++
		}
		txMap.RUnlock()

		if count >= max {
			return result, nil
		}
	}

	return result, nil
}

func contains(ids []uuid.UUID, lookup uuid.UUID) bool {
	for _, id := range ids {
		if bytes.Equal(id[:], lookup[:]) {
			return true
		}
	}

	return false
}

// Clean removes all txs that are older than the specified time. Txs are retained for long enough to
// prevent redownloading them while they continue to propagate, but are no longer needed after nodes
// stop announcing them.
func (m *TxManager) Clean(ctx context.Context, oldest time.Time) error {
	start := time.Now()

	removedCount := 0
	for i := 0; i < 256; i++ {
		m.RLock()
		txMap := m.txMaps[i]
		m.RUnlock()

		txMap.Lock()
		newTxs := make(map[bitcoin.Hash32]*TxData)
		for txid, data := range txMap.txs {
			if data.Latest().After(oldest) {
				newTxs[txid] = data
			} else {
				removedCount++
			}
		}
		txMap.txs = newTxs
		txMap.Unlock()
	}

	logger.ElapsedWithFields(ctx, start, []logger.Field{
		logger.Int("removed_count", removedCount),
	}, "Cleaned tx manager")
	return nil
}

func (tx *TxData) Latest() time.Time {
	tx.RLock()
	defer tx.RUnlock()

	if tx.Received != nil {
		return *tx.Received
	}

	return tx.LastRequested
}
