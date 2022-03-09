package bitcoin_reader

import (
	"context"
	"sync"

	"github.com/tokenized/bitcoin_reader/headers"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/wire"
)

type MockHeaderRepository struct {
	lastHeight                 int
	lastHash                   bitcoin.Hash32
	previousHashes             []bitcoin.Hash32
	newHeadersAvailableChannel *chan *wire.BlockHeader
}

func NewMockHeaderRepository() *MockHeaderRepository {
	lastHeight := 725107
	lastHash, _ := bitcoin.NewHash32FromStr("00000000000000000749cd1e05f963bde1347295de3bea047842d6c7b6a45311")
	previousHash, _ := bitcoin.NewHash32FromStr("000000000000000007de78e08cc3d133f6a9cb2062f31764b7f64643b2575d90")

	return &MockHeaderRepository{
		lastHeight:     lastHeight,
		lastHash:       *lastHash,
		previousHashes: []bitcoin.Hash32{*previousHash},
	}
}

func (m *MockHeaderRepository) GetNewHeadersAvailableChannel() <-chan *wire.BlockHeader {
	result := make(chan *wire.BlockHeader, 1000)

	m.newHeadersAvailableChannel = &result
	return result
}

func (m *MockHeaderRepository) GetVerifyOnlyLocatorHashes(ctx context.Context) ([]bitcoin.Hash32, error) {
	return m.previousHashes, nil
}

func (m *MockHeaderRepository) GetLocatorHashes(ctx context.Context,
	max int) ([]bitcoin.Hash32, error) {
	return m.previousHashes, nil
}

func (m *MockHeaderRepository) Height() int {
	return m.lastHeight
}

func (m *MockHeaderRepository) HashHeight(hash bitcoin.Hash32) int {
	if m.lastHash.Equal(&hash) {
		return m.lastHeight
	}

	return -1
}

func (m *MockHeaderRepository) LastHash() bitcoin.Hash32 {
	return m.lastHash
}

func (m *MockHeaderRepository) LastTime() uint32 {
	return 0
}

func (m *MockHeaderRepository) PreviousHash(hash bitcoin.Hash32) (*bitcoin.Hash32, int) {
	return nil, -1
}

func (m *MockHeaderRepository) VerifyHeader(ctx context.Context, header *wire.BlockHeader) error {
	hash := *header.BlockHash()
	if hash.Equal(&m.lastHash) {
		return nil
	}

	return headers.ErrUnknownHeader
}

func (m *MockHeaderRepository) ProcessHeader(ctx context.Context, header *wire.BlockHeader) error {
	hash := *header.BlockHash()
	if hash.Equal(&m.lastHash) {
		return nil
	}

	return headers.ErrUnknownHeader
}

func (m *MockHeaderRepository) Stop(ctx context.Context) {
	if m.newHeadersAvailableChannel != nil {
		close(*m.newHeadersAvailableChannel)
		m.newHeadersAvailableChannel = nil
	}
}

type MockBlockTxManager struct {
	blocks map[bitcoin.Hash32][]bitcoin.Hash32

	sync.Mutex
}

func NewMockBlockTxManager() *MockBlockTxManager {
	return &MockBlockTxManager{
		blocks: make(map[bitcoin.Hash32][]bitcoin.Hash32),
	}
}

func (m *MockBlockTxManager) FetchBlockTxIDs(ctx context.Context,
	blockHash bitcoin.Hash32) ([]bitcoin.Hash32, bool, error) {
	m.Lock()
	defer m.Unlock()

	txids, exists := m.blocks[blockHash]
	if !exists {
		return nil, false, nil
	}
	return txids, true, nil
}

func (m *MockBlockTxManager) AppendBlockTxIDs(ctx context.Context, blockHash bitcoin.Hash32,
	txids []bitcoin.Hash32) error {
	m.Lock()
	defer m.Unlock()

	existingTxids, exists := m.blocks[blockHash]
	if !exists {
		m.blocks[blockHash] = txids
		return nil
	}

	for _, txid := range txids {
		existingTxids = appendHash32(existingTxids, txid)
	}

	m.blocks[blockHash] = existingTxids
	return nil
}

func appendHash32(hashes []bitcoin.Hash32, hash bitcoin.Hash32) []bitcoin.Hash32 {
	for _, h := range hashes {
		if h.Equal(&hash) {
			return hashes // already exists
		}
	}

	return append(hashes, hash)
}
