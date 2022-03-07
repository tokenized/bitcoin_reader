package headers

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/tokenized/bitcoin_reader/internal/platform/tests"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/storage"
)

func Test_Headers_Clean(t *testing.T) {
	ctx := tests.Context()
	store := storage.NewMockStorage()
	repo := NewRepository(DefaultConfig(), store)
	repo.DisableDifficulty()
	startTime := uint32(time.Now().Unix())
	repo.InitializeWithTimeStamp(startTime)

	MockHeaders(ctx, repo, repo.LastHash(), startTime, 1100)

	otherHashes := make([]bitcoin.Hash32, repo.Height()+1)
	for i := range otherHashes {
		otherHashes[i] = repo.longest.AtHeight(i).Hash
	}

	branchHash, _ := repo.Hash(ctx, 1090)
	t.Logf("Branching after hash %d %s", 1090, branchHash)
	MockHeaders(ctx, repo, *branchHash, startTime+(1090*600), 20)

	if len(repo.branches) != 2 {
		t.Fatalf("Wrong branch count : got %d, want %d", len(repo.branches), 2)
	}

	if repo.longest.Height() != 1110 {
		t.Errorf("Wrong longest branch height : got %d, want %d", repo.longest.Height(), 1110)
	}

	longestHashes := make([]bitcoin.Hash32, repo.Height()+1)
	for i := range longestHashes {
		longestHashes[i] = repo.longest.AtHeight(i).Hash
	}

	var otherBranch *Branch
	if repo.longest == repo.branches[0] {
		otherBranch = repo.branches[1]
	} else {
		otherBranch = repo.branches[0]
	}

	if otherBranch.Height() != 1100 {
		t.Errorf("Wrong longest branch height : got %d, want %d", otherBranch.Height(), 1100)
	}

	if repo.longest.PrunedLowestHeight() != 1091 {
		t.Errorf("Wrong longest branch pruned lowest height : got %d, want %d",
			repo.longest.PrunedLowestHeight(), 1091)
	}

	if otherBranch.PrunedLowestHeight() != 0 {
		t.Errorf("Wrong other branch pruned lowest height : got %d, want %d",
			otherBranch.PrunedLowestHeight(), 0)
	}

	if err := repo.consolidate(ctx); err != nil {
		t.Fatalf("Failed to consolidate : %s", err)
	}

	if repo.longest == repo.branches[0] {
		otherBranch = repo.branches[1]
	} else {
		otherBranch = repo.branches[0]
	}

	if repo.longest.PrunedLowestHeight() != 0 {
		t.Errorf("Wrong longest branch pruned lowest height : got %d, want %d",
			repo.longest.PrunedLowestHeight(), 0)
	}

	if otherBranch.PrunedLowestHeight() != 1091 {
		t.Errorf("Wrong other branch pruned lowest height : got %d, want %d",
			otherBranch.PrunedLowestHeight(), 1091)
	}

	if repo.longest.Height() != 1110 {
		t.Errorf("Wrong longest branch height : got %d, want %d", repo.longest.Height(), 1110)
	}

	for i := 0; i < 1110; i++ {
		if !repo.longest.AtHeight(i).Hash.Equal(&longestHashes[i]) {
			t.Errorf("Wrong hash at height %d : \ngot  : %s\nwant : %s", i,
				repo.longest.AtHeight(i).Hash, longestHashes[i])
		}
	}

	if otherBranch.Height() != 1100 {
		t.Errorf("Wrong other branch height : got %d, want %d", otherBranch.Height(), 1100)
	}

	for i := 0; i < 1100; i++ {
		if !otherBranch.AtHeight(i).Hash.Equal(&otherHashes[i]) {
			t.Errorf("Wrong hash at height %d : \ngot  : %s\nwant : %s", i,
				otherBranch.AtHeight(i).Hash, otherHashes[i])
		}
	}

	if err := repo.saveMainBranch(ctx); err != nil {
		t.Fatalf("Failed to save main branch : %s", err)
	}

	if err := repo.saveBranches(ctx); err != nil {
		t.Fatalf("Failed to save branches : %s", err)
	}

	headerFiles, err := store.List(ctx, headersPath)
	if err != nil {
		t.Fatalf("Failed to list files : %s", err)
	}

	if len(headerFiles) != 5 {
		t.Errorf("Wrong header file count : got %d, want %d", len(headerFiles), 5)
	}

	for _, headerFile := range headerFiles {
		t.Logf("Header file : %s", headerFile)
	}

	data, err := store.Read(ctx, headersFilePath(0))
	if err != nil {
		t.Fatalf("Failed to read first headers file : %s", err)
	}
	buf := bytes.NewReader(data)
	for i := 0; i < headersPerFile; i++ {
		headerData := &HeaderData{}
		if err := headerData.Deserialize(buf); err != nil {
			t.Fatalf("Failed to read header data %d : %s", i, err)
		}

		if !headerData.Hash.Equal(&longestHashes[i]) {
			t.Errorf("Wrong hash at height %d : \ngot  : %s\nwant : %s", i, headerData.Hash,
				longestHashes[i])
		}
	}

	data, err = store.Read(ctx, headersFilePath(1))
	if err != nil {
		t.Fatalf("Failed to read second headers file : %s", err)
	}
	buf = bytes.NewReader(data)
	for i := 1000; i < 1111; i++ {
		headerData := &HeaderData{}
		if err := headerData.Deserialize(buf); err != nil {
			t.Fatalf("Failed to read header data %d : %s", i, err)
		}

		if !headerData.Hash.Equal(&longestHashes[i]) {
			t.Errorf("Wrong hash at height %d : \ngot  : %s\nwant : %s", i, headerData.Hash,
				longestHashes[i])
		}
	}

	if buf.Len() != 0 {
		t.Errorf("Extra header file data left : %d", buf.Len())
	}

	if err := repo.prune(ctx, 500); err != nil {
		t.Fatalf("Failed to prune : %s", err)
	}

	if repo.longest.AtHeight(500) != nil {
		t.Errorf("Height 500 not pruned")
	}

	data, err = store.Read(ctx, fmt.Sprintf("%s/index", branchPath))
	if err != nil {
		t.Fatalf("Failed to read branch index file : %s", err)
	}
	buf = bytes.NewReader(data)
	var indexCount uint32
	if err := binary.Read(buf, endian, &indexCount); err != nil {
		t.Fatalf("Failed to read branch index count : %s", err)
	}

	if indexCount != 2 {
		t.Fatalf("Wrong branch index count : got %d, want %d", indexCount, 2)
	}

	firstBranchHash := &bitcoin.Hash32{}
	if err := firstBranchHash.Deserialize(buf); err != nil {
		t.Fatalf("Failed to read first branch hash : %s", err)
	}

	secondBranchHash := &bitcoin.Hash32{}
	if err := secondBranchHash.Deserialize(buf); err != nil {
		t.Fatalf("Failed to read second branch hash : %s", err)
	}

	if buf.Len() != 0 {
		t.Errorf("Extra branch index data left : %d", buf.Len())
	}

	firstBranch, err := LoadBranch(ctx, store, *firstBranchHash)
	if err != nil {
		t.Fatalf("Failed to load first branch : %s", err)
	}

	t.Logf("\nFirst Branch :" + firstBranch.String("  "))

	if firstBranch.PrunedLowestHeight() != 0 {
		t.Errorf("Wrong first branch pruned lowest height : got %d, want %d",
			firstBranch.PrunedLowestHeight(), 0)
	}

	if firstBranch.Height() != 1110 {
		t.Errorf("Wrong first branch height : got %d, want %d", firstBranch.Height(), 1110)
	}

	if !firstBranch.Last().Hash.Equal(&repo.longest.Last().Hash) {
		t.Errorf("Wrong first branch last hash : \ngot  : %s\nwant : %s", firstBranch.Last().Hash,
			repo.longest.Last().Hash)
	}

	for i := 0; i < 1110; i++ {
		if !firstBranch.AtHeight(i).Hash.Equal(&longestHashes[i]) {
			t.Errorf("Wrong hash at height %d : \ngot  : %s\nwant : %s", i,
				firstBranch.AtHeight(i).Hash, longestHashes[i])
		}
	}

	secondBranch, err := LoadBranch(ctx, store, *secondBranchHash)
	if err != nil {
		t.Fatalf("Failed to load second branch : %s", err)
	}

	t.Logf("\nSecond Branch :" + secondBranch.String("  "))

	if secondBranch.PrunedLowestHeight() != 1091 {
		t.Errorf("Wrong second branch pruned lowest height : got %d, want %d",
			secondBranch.PrunedLowestHeight(), 1091)
	}

	if secondBranch.Height() != 1100 {
		t.Errorf("Wrong second branch height : got %d, want %d", secondBranch.Height(), 1100)
	}

	for i := 1091; i < 1100; i++ {
		if !secondBranch.AtHeight(i).Hash.Equal(&otherHashes[i]) {
			t.Errorf("Wrong hash at height %d : \ngot  : %s\nwant : %s", i,
				secondBranch.AtHeight(i).Hash, otherHashes[i])
		}
	}

	if err := repo.Save(ctx); err != nil {
		t.Fatalf("Failed to save repo : %s", err)
	}

	loadedRepo := NewRepository(DefaultConfig(), store)
	if err := loadedRepo.load(ctx, 500); err != nil {
		t.Fatalf("Failed to load repo : %s", err)
	}

	firstBranch = loadedRepo.branches[0]
	if firstBranch.PrunedLowestHeight() != 610 {
		t.Errorf("Wrong first branch pruned lowest height : got %d, want %d",
			firstBranch.PrunedLowestHeight(), 610)
	}

	if firstBranch.Height() != 1110 {
		t.Errorf("Wrong first branch height : got %d, want %d", firstBranch.Height(), 1110)
	}

	for i := firstBranch.PrunedLowestHeight(); i <= firstBranch.Height(); i++ {
		if !firstBranch.AtHeight(i).Hash.Equal(&longestHashes[i]) {
			t.Errorf("Wrong hash at height %d : \ngot  : %s\nwant : %s", i,
				firstBranch.AtHeight(i).Hash, longestHashes[i])
		}
	}

	secondBranch = loadedRepo.branches[1]
	if secondBranch.PrunedLowestHeight() != 1091 {
		t.Errorf("Wrong second branch pruned lowest height : got %d, want %d",
			secondBranch.PrunedLowestHeight(), 1091)
	}

	if secondBranch.Height() != 1100 {
		t.Errorf("Wrong second branch height : got %d, want %d", secondBranch.Height(), 1100)
	}

	for i := firstBranch.PrunedLowestHeight(); i <= secondBranch.Height(); i++ {
		if !secondBranch.AtHeight(i).Hash.Equal(&otherHashes[i]) {
			t.Errorf("Wrong hash at height %d : \ngot  : %s\nwant : %s", i,
				secondBranch.AtHeight(i).Hash, otherHashes[i])
		}
	}
}

func Test_genesisHeaders(t *testing.T) {
	main := genesisHeader(bitcoin.MainNet)
	const mainHash = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
	if main.BlockHash().String() != mainHash {
		t.Errorf("Wrong mainnet genesis header hash : \ngot  : %s\nwant : %s", main.BlockHash(),
			mainHash)
	}
	t.Logf("Main Genesis Hash : %s", main.BlockHash())

	test := genesisHeader(bitcoin.TestNet)
	const testHash = "000000000933ea01ad0ee984209779baaec3ced90fa3f408719526f8d77f4943"
	if test.BlockHash().String() != testHash {
		t.Errorf("Wrong testnet genesis header hash : \ngot  : %s\nwant : %s", test.BlockHash(),
			testHash)
	}
	t.Logf("Test Genesis Hash : %s", test.BlockHash())

}
