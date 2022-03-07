package headers

import (
	"math/rand"
	"testing"

	"github.com/tokenized/bitcoin_reader/internal/platform/tests"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/pkg/wire"
)

func Test_Splits_init_Order(t *testing.T) {
	store := storage.NewMockStorage()
	repo := NewRepository(DefaultConfig(), store)

	// Check order is highest to lowest
	for i, split := range repo.splits {
		if i == 0 {
			continue
		}

		if repo.splits[i-1].Height < split.Height {
			t.Errorf("Split %d (height %d) is before split %d (height %d)", i-1,
				repo.splits[i-1].Height, i, split.Height)
		}
	}
}

func Test_Splits_init_Values(t *testing.T) {
	store := storage.NewMockStorage()
	repo := NewRepository(DefaultConfig(), store)

	// Verify correct hashes
	btcBefore, _ := bitcoin.NewHash32FromStr("0000000000000000011865af4122fe3b144e2cbeea86142e8ff2fb4107352d43")
	btcAfter, _ := bitcoin.NewHash32FromStr("00000000000000000019f112ec0a9982926f1258cdcc558dd7c3b7e5dc7fa148")
	btcHeight := 478559
	btcFound := false

	bchBefore, _ := bitcoin.NewHash32FromStr("00000000000000000102d94fde9bd0807a2cc7582fe85dd6349b73ce4e8d9322")
	bchAfter, _ := bitcoin.NewHash32FromStr("0000000000000000004626ff6e3b936941d341c5932ece4357eeccac44e6d56c")
	bchHeight := 556767
	bchFound := false

	for _, split := range repo.splits {
		t.Logf("Split %s height %d, before %s, after, %s", split.Name, split.Height,
			split.BeforeHash, split.AfterHash)

		if split.Name == SplitNameBTC {
			btcFound = true
			if split.Height != btcHeight {
				t.Errorf("Wrong %s split height : got %d, want %d", split.Name, split.Height,
					btcHeight)
			}

			if !split.BeforeHash.Equal(btcBefore) {
				t.Errorf("Wrong %s split before hash : got %s, want %s", split.Name,
					split.BeforeHash, btcBefore)
			}

			if !split.AfterHash.Equal(btcAfter) {
				t.Errorf("Wrong %s split after hash : got %s, want %s", split.Name,
					split.AfterHash, btcBefore)
			}
		}
		if split.Name == SplitNameBCH {
			bchFound = true
			if split.Height != bchHeight {
				t.Errorf("Wrong %s split height : got %d, want %d", split.Name, split.Height,
					bchHeight)
			}

			if !split.BeforeHash.Equal(bchBefore) {
				t.Errorf("Wrong %s split before hash : got %s, want %s", split.Name,
					split.BeforeHash, bchBefore)
			}

			if !split.AfterHash.Equal(bchAfter) {
				t.Errorf("Wrong %s split after hash : got %s, want %s", split.Name,
					split.AfterHash, bchBefore)
			}
		}
	}

	if !btcFound {
		t.Errorf("%s Split not found", SplitNameBTC)
	}
	if !bchFound {
		t.Errorf("%s Split not found", SplitNameBCH)
	}
}

func Test_Splits_init_GetLocatorHashes_AfterSplits(t *testing.T) {
	if !testing.Verbose() {
		t.Skip() // Don't want to redownload the block all the time
	}
	ctx := tests.Context()

	store := storage.NewMockStorage()
	repo := NewRepository(DefaultConfig(), store)
	repo.DisableDifficulty()
	repo.DisableSplitProtection()

	repo.InitializeWithGenesis()

	// Mock 575000 headers
	previous := repo.LastHash()
	timestamp := repo.longest.Last().Header.Timestamp
	for i := 0; i < 575000; i++ {
		header := &wire.BlockHeader{
			Version:   1,
			PrevBlock: previous,
			Timestamp: timestamp,
			Bits:      0x1d00ffff,
			Nonce:     rand.Uint32(),
		}
		rand.Read(header.MerkleRoot[:])

		if err := repo.ProcessHeader(ctx, header); err != nil {
			t.Fatalf("Failed to add header : %s", err)
		}

		previous = *header.BlockHash()
		timestamp += 600
	}

	heightHashes := repo.longest.GetLocatorHashes(repo.splits, 100, 50)
	previousHeight := repo.Height()
	btcFound := false
	bchFound := false
	for i, heightHash := range heightHashes {
		height := repo.HashHeight(heightHash.Hash)
		if height != -1 {
			t.Logf("Hash  %06d : %s", height, heightHash.Hash)
			if height > previousHeight {
				t.Errorf("Hash %d at offset after lower height : height %d, previous %d", i, height,
					previousHeight)
			}
			previousHeight = height
			continue
		}

		for _, split := range repo.splits {
			if !heightHash.Hash.Equal(&split.BeforeHash) {
				continue
			}

			t.Logf("Split %d : %s", split.Height, heightHash.Hash)

			if split.Name == SplitNameBCH {
				bchFound = true
			} else if split.Name == SplitNameBTC {
				btcFound = true
			} else {
				t.Logf("Other split found : %s", split.Name)
			}

			if split.Height > previousHeight {
				t.Errorf("%s split at offset after lower height : height %d, previous %d",
					split.Name, split.Height, previousHeight)
			}

			previousHeight = split.Height
			break
		}
	}

	if !btcFound {
		t.Errorf("%s Split not found", SplitNameBTC)
	}
	if !bchFound {
		t.Errorf("%s Split not found", SplitNameBCH)
	}
}

func Test_Splits_GetLocatorHashes_BeforeSplits(t *testing.T) {
	if !testing.Verbose() {
		t.Skip() // Don't want to redownload the block all the time
	}
	ctx := tests.Context()

	store := storage.NewMockStorage()
	repo := NewRepository(DefaultConfig(), store)
	repo.DisableDifficulty()

	repo.InitializeWithGenesis()

	// Mock 575000 headers
	previous := repo.LastHash()
	var merklehash bitcoin.Hash32
	timestamp := genesisHeader(bitcoin.MainNet).Timestamp
	var nonce uint32
	for i := 0; i < 300000; i++ {
		rand.Read(merklehash[:])
		nonce = rand.Uint32()

		header := &wire.BlockHeader{
			Version:    1,
			PrevBlock:  previous,
			MerkleRoot: merklehash,
			Timestamp:  timestamp,
			Bits:       0x1d00ffff,
			Nonce:      nonce,
		}

		if err := repo.ProcessHeader(ctx, header); err != nil {
			t.Fatalf("Failed to add header : %s", err)
		}

		previous = *header.BlockHash()
		timestamp += 600
	}

	heightHashes := repo.longest.GetLocatorHashes(repo.splits, 100, 50)
	previousHeight := repo.Height()
	btcFound := false
	bchFound := false
	for i, heightHash := range heightHashes {
		height := repo.HashHeight(heightHash.Hash)
		if height != -1 {
			t.Logf("Hash %06d : %s", height, heightHash.Hash)
			if height > previousHeight {
				t.Errorf("Hash %d at offset after lower height : height %d, previous %d", i, height,
					previousHeight)
			}
			previousHeight = height
			continue
		}

		for _, split := range repo.splits {
			if !heightHash.Hash.Equal(&split.BeforeHash) {
				continue
			}

			t.Logf("Split Hash %d : %s", split.Height, heightHash.Hash)

			if split.Name == SplitNameBCH {
				bchFound = true
			} else if split.Name == SplitNameBTC {
				btcFound = true
			} else {
				t.Logf("Other split found : %s", split.Name)
			}

			if split.Height > previousHeight {
				t.Errorf("%s split at offset after lower height : height %d, previous %d",
					split.Name, split.Height, previousHeight)
			}

			previousHeight = split.Height
			break
		}
	}

	if btcFound {
		t.Errorf("%s Split found", SplitNameBTC)
	}
	if bchFound {
		t.Errorf("%s Split found", SplitNameBCH)
	}
}
