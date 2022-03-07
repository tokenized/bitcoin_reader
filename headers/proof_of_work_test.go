package headers

import (
	"encoding/json"
	"math/big"
	"os"
	"testing"

	"github.com/tokenized/bitcoin_reader/internal/platform/tests"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/pkg/wire"

	"github.com/pkg/errors"
)

func Test_CalculateTarget_725000(t *testing.T) {
	ctx := tests.Context()

	file, err := os.Open("test_fixtures/headers_725000.txt")
	if err != nil {
		t.Fatalf("Failed to open file : %s", err)
	}

	store := storage.NewMockStorage()
	headersRepo := NewRepository(DefaultConfig(), store)
	headersRepo.DisableDifficulty()
	height := 725000

	var headers []*wire.BlockHeader
	if err := json.NewDecoder(file).Decode(&headers); err != nil {
		t.Fatalf("Failed to read headers : %s", err)
	}

	work := &big.Int{}
	work.SetString("134b2eb2b14bbedbad9a14b", 16) // Chain work from 724999

	for i, header := range headers {
		if i == 150 {
			// Re-enable after there are enough previous headers to calculate it
			headersRepo.EnableDifficulty()
		}
		if i > 150 {
			target, err := headersRepo.branches[0].Target(ctx, height)
			if err != nil {
				t.Fatalf("Failed to calculate target : %s", err)
			}

			bits := bitcoin.ConvertToBits(target, bitcoin.MaxBits)
			if bits != header.Bits {
				t.Fatalf("Wrong calculated target bits for height %d : got 0x%08x, want 0x%08x",
					height, bits, header.Bits)
			}

			if !header.WorkIsValid() {
				t.Fatalf("Failed to verify header work at height %d", height)
			}

			// Verify work was done on header
			actualWork := header.BlockHash().Value()
			// t.Logf("Work   : %64s", actualWork.Text(16))
			// t.Logf("Target : %64s", target.Text(16))

			if actualWork.Cmp(target) > 0 {
				t.Fatalf("Not enough work for height %d", height)
			}
		}

		if i == 0 {
			if err := headersRepo.MockLatest(ctx, header, height, work); err != nil {
				t.Fatalf("Failed to initialize headers repo : %s", err)
			}
		} else {
			if err := headersRepo.ProcessHeader(ctx, header); err != nil {
				t.Fatalf("Failed to add header : %s", err)
			}
		}

		blockWork := bitcoin.ConvertToWork(bitcoin.ConvertToDifficulty(header.Bits))
		work.Add(work, blockWork)

		_, lastWork, err := headersRepo.branches[0].TimeAndWork(ctx, height)
		if err != nil {
			t.Fatalf("Failed to get header stats %d : %s", height, err)
		}

		if work.Cmp(lastWork) != 0 {
			t.Fatalf("Wrong result work for block %d : \ngot  %s \nwant %s", height,
				work.Text(16), lastWork.Text(16))
		}

		height++
	}

	wantWork := &big.Int{}
	wantWork.SetString("134e46a6635192307dfa78b", 16) // Chain work from 725835
	if work.Cmp(wantWork) != 0 {
		t.Errorf("Wrong result work : \ngot  %s \nwant %s", work.Text(16), wantWork.Text(16))
	}
}

// Test_ChainSplit_556000_BSV ensure that the BSV split header is accepted.
func Test_ChainSplit_556000_BSV(t *testing.T) {
	ctx := tests.Context()

	file, err := os.Open("test_fixtures/headers_556000.txt")
	if err != nil {
		t.Fatalf("Failed to open file : %s", err)
	}

	store := storage.NewMockStorage()
	headersRepo := NewRepository(DefaultConfig(), store)
	headersRepo.DisableDifficulty()
	height := 556000

	var headers []*wire.BlockHeader
	if err := json.NewDecoder(file).Decode(&headers); err != nil {
		t.Fatalf("Failed to read headers : %s", err)
	}

	work := &big.Int{}
	work.SetString("d167cf38dd7a9c078a40d5", 16) // Chain work from 555999

	for i, header := range headers {
		if i == 150 {
			// Re-enable after there are enough previous headers to calculate it
			headersRepo.EnableDifficulty()
		}
		if i > 150 {
			target, err := headersRepo.branches[0].Target(ctx, height)
			if err != nil {
				t.Fatalf("Failed to calculate target : %s", err)
			}

			bits := bitcoin.ConvertToBits(target, bitcoin.MaxBits)
			if bits != header.Bits {
				t.Fatalf("Wrong calculated target bits for height %d : got 0x%08x, want 0x%08x",
					height, bits, header.Bits)
			}

			if !header.WorkIsValid() {
				t.Fatalf("Failed to verify header work at height %d", height)
			}

			// Verify work was done on header
			actualWork := header.BlockHash().Value()
			// t.Logf("Work   : %64s", actualWork.Text(16))
			// t.Logf("Target : %64s", target.Text(16))
			// t.Logf("Accumulated Work %d : %64s", height, work.Text(16))

			if actualWork.Cmp(target) > 0 {
				t.Fatalf("Not enough work for height %d", height)
			}
		}

		if i == 0 {
			if err := headersRepo.MockLatest(ctx, header, height, work); err != nil {
				t.Fatalf("Failed to initialize headers repo : %s", err)
			}
		} else {
			if err := headersRepo.ProcessHeader(ctx, header); err != nil {
				t.Fatalf("Failed to add header : %s", err)
			}
		}

		blockWork := bitcoin.ConvertToWork(bitcoin.ConvertToDifficulty(header.Bits))
		work.Add(work, blockWork)

		_, lastWork, err := headersRepo.branches[0].TimeAndWork(ctx, height)
		if err != nil {
			t.Fatalf("Failed to get header stats : %s", err)
		}

		if work.Cmp(lastWork) != 0 {
			t.Fatalf("Wrong result work for block %d : \ngot  %s \nwant %s", height,
				work.Text(16), lastWork.Text(16))
		}

		height++
	}

	t.Logf("Added headers to height : %d", headersRepo.Height())

	wantWork := &big.Int{}
	wantWork.SetString("d555fcdba6ad0ba9b95c36", 16) // Chain work from 557985
	if work.Cmp(wantWork) != 0 {
		t.Errorf("Wrong result work : \ngot  %s \nwant %s", work.Text(16), wantWork.Text(16))
	}
}

// Test_ChainSplit_556000_BCH ensure that the BCH split header is rejected with ErrWrongChain.
func Test_ChainSplit_556000_BCH(t *testing.T) {
	ctx := tests.Context()

	file, err := os.Open("test_fixtures/headers_556000.txt")
	if err != nil {
		t.Fatalf("Failed to open file : %s", err)
	}

	store := storage.NewMockStorage()
	headersRepo := NewRepository(DefaultConfig(), store)
	headersRepo.DisableDifficulty()
	height := 556000

	var headers []*wire.BlockHeader
	if err := json.NewDecoder(file).Decode(&headers); err != nil {
		t.Fatalf("Failed to read headers : %s", err)
	}

	if len(headers) < 1000 {
		t.Fatalf("Not enough headers read : %d", len(headers))
	}

	splitHash, _ := bitcoin.NewHash32FromStr("000000000000000001d956714215d96ffc00e0afda4cd0a96c96f8d802b1662b")
	if !headers[767].BlockHash().Equal(splitHash) {
		t.Fatalf("Wrong header at height 556767 : \ngot  %s \nwant %s", headers[767].BlockHash(),
			splitHash)
	}

	bchMerkleRoot, _ := bitcoin.NewHash32FromStr("1cf31105bd6b1b4dba9ae55290ec06fff15b4567ec62a6e3863409bb3efd1944")
	bchSplitHeader := &wire.BlockHeader{
		Version:    0x20000000,
		PrevBlock:  *headers[766].BlockHash(),
		MerkleRoot: *bchMerkleRoot,
		Timestamp:  1542304936, // Nov 15, 2018 6:02 PM
		Bits:       402792411,
		Nonce:      3911120513,
	}

	bchHash, _ := bitcoin.NewHash32FromStr("0000000000000000004626ff6e3b936941d341c5932ece4357eeccac44e6d56c")
	if !bchHash.Equal(bchSplitHeader.BlockHash()) {
		t.Errorf("Incorrect BCH header hash : %s", bchSplitHeader.BlockHash())
	}

	headers = append(headers[:767], bchSplitHeader)

	work := &big.Int{}
	work.SetString("d167cf38dd7a9c078a40d5", 16) // Chain work from 555999

	for i, header := range headers {
		if i == 150 {
			// Re-enable after there are enough previous headers to calculate it
			headersRepo.EnableDifficulty()
		}
		if i > 150 {
			target, err := headersRepo.branches[0].Target(ctx, height)
			if err != nil {
				t.Fatalf("Failed to calculate target : %s", err)
			}

			bits := bitcoin.ConvertToBits(target, bitcoin.MaxBits)
			if bits != header.Bits {
				t.Fatalf("Wrong calculated target bits for height %d : got 0x%08x, want 0x%08x",
					height, bits, header.Bits)
			}

			if !header.WorkIsValid() {
				t.Fatalf("Failed to verify header work at height %d", height)
			}

			// Verify work was done on header
			actualWork := header.BlockHash().Value()
			// t.Logf("Work   : %64s", actualWork.Text(16))
			// t.Logf("Target : %64s", target.Text(16))
			// t.Logf("Accumulated Work %d : %64s", height, work.Text(16))

			if actualWork.Cmp(target) > 0 {
				t.Fatalf("Not enough work for height %d", height)
			}
		}

		if i == 0 {
			if err := headersRepo.MockLatest(ctx, header, height, work); err != nil {
				t.Fatalf("Failed to initialize headers repo : %s", err)
			}
		} else if height == 556767 {
			if err := headersRepo.ProcessHeader(ctx, header); err == nil {
				t.Fatalf("Added invalid header")
			} else if errors.Cause(err) != ErrWrongChain {
				t.Errorf("Wrong error : %s", err)
			}
			break
		} else {
			if err := headersRepo.ProcessHeader(ctx, header); err != nil {
				t.Fatalf("Failed to add header : %s", err)
			}
		}

		blockWork := bitcoin.ConvertToWork(bitcoin.ConvertToDifficulty(header.Bits))
		work.Add(work, blockWork)

		_, lastWork, err := headersRepo.branches[0].TimeAndWork(ctx, height)
		if err != nil {
			t.Fatalf("Failed to get header stats : %s", err)
		}

		if work.Cmp(lastWork) != 0 {
			t.Fatalf("Wrong result work for block %d : \ngot  %s \nwant %s", height,
				work.Text(16), lastWork.Text(16))
		}

		height++
	}

	t.Logf("Added headers to height : %d", headersRepo.Height())

	wantWork := &big.Int{}
	wantWork.SetString("d3367b433e911be0f8dbb9", 16) // Chain work from 556766
	if work.Cmp(wantWork) != 0 {
		t.Errorf("Wrong result work : \ngot  %s \nwant %s", work.Text(16), wantWork.Text(16))
	}
}
