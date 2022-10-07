package headers

import (
	"context"
	"fmt"
	"math/big"
	"math/rand"

	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/wire"
)

func MockHeaders(ctx context.Context, repo *Repository, afterHash bitcoin.Hash32, timestamp uint32,
	count int) []*wire.BlockHeader {

	var headers []*wire.BlockHeader
	previousHash := afterHash
	for i := 0; i < count; i++ {
		header := &wire.BlockHeader{
			Version:   1,
			PrevBlock: previousHash,
			Timestamp: timestamp,
			Bits:      0x1d00ffff,
			Nonce:     rand.Uint32(),
		}
		rand.Read(header.MerkleRoot[:])

		if err := repo.ProcessHeader(ctx, header); err != nil {
			panic(fmt.Sprintf("add header %d: %s", i, err))
		}

		headers = append(headers, header)
		previousHash = *header.BlockHash()
		timestamp += 600
	}

	return headers
}

func MockHeadersOnBranch(branch *Branch, count int) {
	previousHash := branch.Last().Hash
	timestamp := branch.Last().Header.Timestamp

	for i := 0; i < count; i++ {
		header := &wire.BlockHeader{
			Version:   1,
			PrevBlock: previousHash,
			Timestamp: timestamp,
			Bits:      0x1d00ffff,
			Nonce:     rand.Uint32(),
		}
		rand.Read(header.MerkleRoot[:])

		if !branch.Add(header) {
			panic(fmt.Sprintf("add header %d", i))
		}

		previousHash = *header.BlockHash()
		timestamp += 600
	}
}

// Initialize with genesis header with specified timestamp.
func (repo *Repository) InitializeWithTimeStamp(timestamp uint32) error {
	repo.Lock()
	defer repo.Unlock()

	header := &wire.BlockHeader{
		Version:   1,
		Timestamp: timestamp,
		Bits:      0x1d00ffff,
		Nonce:     rand.Uint32(),
	}
	rand.Read(header.MerkleRoot[:])

	repo.longest, _ = NewBranch(nil, -1, header)
	repo.branches = Branches{repo.longest}
	return nil
}

func (repo *Repository) DisableDifficulty() {
	repo.Lock()
	defer repo.Unlock()

	repo.disableDifficulty = true
}

func (repo *Repository) EnableDifficulty() {
	repo.Lock()
	defer repo.Unlock()

	repo.disableDifficulty = false
}

func (repo *Repository) DisableSplitProtection() {
	repo.Lock()
	defer repo.Unlock()

	repo.disableSplitProtection = true
}

func (repo *Repository) EnableSplitProtection() {
	repo.Lock()
	defer repo.Unlock()

	repo.disableSplitProtection = false
}

// MockLatest is used only during testing to set a header as the latest.
func (repo *Repository) MockLatest(ctx context.Context, header *wire.BlockHeader,
	height int, work *big.Int) error {
	repo.Lock()
	defer repo.Unlock()

	mockBranch, _ := NewBranch(nil, height-1, header)
	mockBranch.headers[0].AccumulatedWork = work
	repo.branches = Branches{mockBranch}
	repo.longest = mockBranch

	return nil
}
