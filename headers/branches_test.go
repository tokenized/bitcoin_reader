package headers

import (
	"math/rand"
	"testing"

	"github.com/tokenized/bitcoin_reader/internal/platform/tests"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/pkg/wire"
)

func Test_Branches_General(t *testing.T) {
	genesis := genesisHeader(bitcoin.MainNet)

	initialBranch, _ := NewBranch(nil, -1, genesis)
	if initialBranch == nil {
		t.Fatalf("Failed to create initial branch")
	}

	// Add some headers
	previousHash := initialBranch.Last().Hash
	timestamp := initialBranch.Last().Header.Timestamp
	headers := make([]*wire.BlockHeader, 11)
	headers[0] = genesis
	for i := 1; i <= 10; i++ {
		header := &wire.BlockHeader{
			Version:   1,
			PrevBlock: previousHash,
			Timestamp: timestamp,
			Bits:      0x1d00ffff,
			Nonce:     rand.Uint32(),
		}
		rand.Read(header.MerkleRoot[:])
		headers[i] = header
		t.Logf("Header at %02d : %s", i, header.BlockHash())

		if !initialBranch.Add(header) {
			t.Fatalf("Failed to add header %d", i)
		}

		previousHash = *header.BlockHash()
		timestamp += 600
	}

	if !initialBranch.Last().Hash.Equal(&previousHash) {
		t.Fatalf("Wrong last hash : \ngot  : %s\nwant : %s", initialBranch.Last().Hash,
			previousHash)
	}

	if initialBranch.Height() != 10 {
		t.Fatalf("Wrong initial branch height : got %d, want %d", initialBranch.Height(), 10)
	}

	at := initialBranch.AtHeight(5)
	if !at.Hash.Equal(headers[5].BlockHash()) {
		t.Fatalf("Wrong hash at 5 : \ngot  : %s\nwant : %s", at.Hash, headers[5].BlockHash())
	}

	branchHash := at.Hash

	branchHeader := &wire.BlockHeader{
		Version:   1,
		PrevBlock: at.Hash,
		Timestamp: timestamp,
		Bits:      0x1d00ffff,
		Nonce:     rand.Uint32(),
	}
	rand.Read(branchHeader.MerkleRoot[:])
	timestamp += 600

	branches := Branches{initialBranch}
	foundBranch, foundHeight := branches.Find(branchHeader.PrevBlock)
	if foundBranch == nil {
		t.Fatalf("Failed to find branch")
	}
	if foundHeight != 5 {
		t.Fatalf("Wrong found height : got %d, want %d", foundHeight, 5)
	}

	branchAt5, err := NewBranch(initialBranch, 5, branchHeader)
	if err != nil {
		t.Fatalf("Failed to create branch at 5 : %s", err)
	}
	branches = append(branches, branchAt5)

	foundBranch, foundHeight = branches.Find(*branchHeader.BlockHash())
	if foundBranch == nil {
		t.Fatalf("Failed to find branch")
	}
	if foundBranch != branchAt5 {
		t.Fatalf("Wrong branch found")
	}
	if foundHeight != 6 {
		t.Fatalf("Wrong found height : got %d, want %d", foundHeight, 6)
	}

	nextBranchHeader := &wire.BlockHeader{
		Version:   1,
		PrevBlock: *branchHeader.BlockHash(),
		Timestamp: timestamp,
		Bits:      0x1d00ffff,
		Nonce:     rand.Uint32(),
	}
	rand.Read(nextBranchHeader.MerkleRoot[:])
	timestamp += 600

	if !branchAt5.Add(nextBranchHeader) {
		t.Fatalf("Failed to add header to branch at 5")
	}

	if availableHeight := initialBranch.AvailableHeight(); availableHeight != 0 {
		t.Errorf("Wrong available height for initial branch : got %d, want %d", availableHeight, 0)
	}

	if availableHeight := branchAt5.AvailableHeight(); availableHeight != 0 {
		t.Errorf("Wrong available height for branch at 5 : got %d, want %d", availableHeight, 0)
	}

	foundBranch, foundHeight = branches.Find(*nextBranchHeader.BlockHash())
	if foundBranch == nil {
		t.Fatalf("Failed to find branch")
	}
	if foundBranch != branchAt5 {
		t.Fatalf("Wrong branch found")
	}
	if foundHeight != 7 {
		t.Fatalf("Wrong found height : got %d, want %d", foundHeight, 7)
	}

	getGenesis := branchAt5.AtHeight(0)
	if getGenesis == nil {
		t.Fatalf("Failed to get genesis from branch at 5")
	}
	if !getGenesis.Hash.Equal(genesis.BlockHash()) {
		t.Fatalf("Wrong genesis hash : \ngot  : %s\nwant : %s", getGenesis.Hash,
			genesis.BlockHash())
	}

	getFirst := branchAt5.AtHeight(1)
	if getFirst == nil {
		t.Fatalf("Failed to get genesis from branch at 5")
	}
	if !getFirst.Hash.Equal(headers[1].BlockHash()) {
		t.Fatalf("Wrong first hash : \ngot  : %s\nwant : %s", getFirst.Hash,
			headers[1].BlockHash())
	}

	intersectHash := initialBranch.IntersectHash(branchAt5)
	if !intersectHash.Equal(&branchHash) {
		t.Errorf("Wrong intersect hash : \ngot  : %s\nwant : %s", intersectHash, branchHash)
	}

	intersectHash = branchAt5.IntersectHash(initialBranch)
	if !intersectHash.Equal(&branchHash) {
		t.Errorf("Wrong intersect hash : \ngot  : %s\nwant : %s", intersectHash, branchHash)
	}
}

func Test_Branches_Prune(t *testing.T) {
	genesis := genesisHeader(bitcoin.MainNet)

	initialBranch, _ := NewBranch(nil, -1, genesis)
	if initialBranch == nil {
		t.Fatalf("Failed to create initial branch")
	}

	// Add some headers
	previousHash := initialBranch.Last().Hash
	timestamp := initialBranch.Last().Header.Timestamp
	headers := make([]*wire.BlockHeader, 11)
	headers[0] = genesis
	for i := 1; i <= 10; i++ {
		header := &wire.BlockHeader{
			Version:   1,
			PrevBlock: previousHash,
			Timestamp: timestamp,
			Bits:      0x1d00ffff,
			Nonce:     rand.Uint32(),
		}
		rand.Read(header.MerkleRoot[:])
		headers[i] = header
		t.Logf("Header at %02d : %s", i, header.BlockHash())

		if !initialBranch.Add(header) {
			t.Fatalf("Failed to add header %d", i)
		}

		previousHash = *header.BlockHash()
		timestamp += 600
	}

	if availableHeight := initialBranch.AvailableHeight(); availableHeight != 0 {
		t.Errorf("Wrong available height for initial branch : got %d, want %d", availableHeight, 0)
	}

	initialBranch.Prune(5)

	if height := initialBranch.Height(); height != 10 {
		t.Errorf("Wrong height : got %d, want %d", height, 10)
	}

	if prunedHeight := initialBranch.PrunedLowestHeight(); prunedHeight != 5 {
		t.Errorf("Wrong pruned lowest height : got %d, want %d", prunedHeight, 5)
	}

	if availableHeight := initialBranch.AvailableHeight(); availableHeight != 5 {
		t.Errorf("Wrong available height : got %d, want %d", availableHeight, 5)
	}

	for i := 0; i <= 4; i++ {
		if initialBranch.AtHeight(i) != nil {
			t.Errorf("%d should be pruned", i)
		} else {
			t.Logf("Verified height %d is pruned", i)
		}

		if initialBranch.Find(*headers[i].BlockHash()) != -1 {
			t.Errorf("Should not find pruned header %d", i)
		} else {
			t.Logf("Verified find %d is pruned", i)
		}
	}

	for i := 5; i <= 10; i++ {
		data := initialBranch.AtHeight(i)
		if data == nil {
			t.Errorf("Missing header at %d", i)
			continue
		}

		t.Logf("Header at %02d : %s", i, data.Hash)
		if !data.Hash.Equal(headers[i].BlockHash()) {
			t.Errorf("Wrong hash at height %d : \ngot  : %s\nwant : %s", i, data.Hash,
				headers[i].BlockHash())
		}

		if height := initialBranch.Find(*headers[i].BlockHash()); height != i {
			t.Errorf("Find header at wrong height : got %d, want %d", height, i)
		}
	}
}

func Test_Branches_Save(t *testing.T) {
	ctx := tests.Context()
	store := storage.NewMockStorage()
	genesis := genesisHeader(bitcoin.MainNet)

	initialBranch, _ := NewBranch(nil, -1, genesis)
	if initialBranch == nil {
		t.Fatalf("Failed to create initial branch")
	}

	// Add some headers
	previousHash := initialBranch.Last().Hash
	timestamp := initialBranch.Last().Header.Timestamp
	headers := make([]*wire.BlockHeader, 11)
	headers[0] = genesis
	for i := 1; i <= 10; i++ {
		header := &wire.BlockHeader{
			Version:   1,
			PrevBlock: previousHash,
			Timestamp: timestamp,
			Bits:      0x1d00ffff,
			Nonce:     rand.Uint32(),
		}
		rand.Read(header.MerkleRoot[:])
		headers[i] = header
		t.Logf("Header at %02d : %s", i, header.BlockHash())

		if !initialBranch.Add(header) {
			t.Fatalf("Failed to add header %d", i)
		}

		previousHash = *header.BlockHash()
		timestamp += 600
	}

	if err := initialBranch.Save(ctx, store); err != nil {
		t.Fatalf("Failed to save initial branch : %s", err)
	}

	initialBranch.Prune(5)

	for i := 11; i <= 15; i++ {
		header := &wire.BlockHeader{
			Version:   1,
			PrevBlock: previousHash,
			Timestamp: timestamp,
			Bits:      0x1d00ffff,
			Nonce:     rand.Uint32(),
		}
		rand.Read(header.MerkleRoot[:])
		headers = append(headers, header)
		t.Logf("Header at %02d : %s", i, header.BlockHash())

		if !initialBranch.Add(header) {
			t.Fatalf("Failed to add header %d", i)
		}

		previousHash = *header.BlockHash()
		timestamp += 600
	}

	if height := initialBranch.Height(); height != 15 {
		t.Errorf("Wrong height after prune and append : got %d, want %d", height, 15)
	}

	if err := initialBranch.Save(ctx, store); err != nil {
		t.Fatalf("Failed to save/append initial branch : %s", err)
	}

	readBranch, err := LoadBranch(ctx, store, *initialBranch.firstHeader.BlockHash())
	if err != nil {
		t.Fatalf("Failed to load branch : %s", err)
	}

	if height := readBranch.Height(); height != initialBranch.Height() {
		t.Errorf("Wrong read branch height : got %d, want %d", height, initialBranch.Height())
	}

	for i, header := range headers {
		data := readBranch.AtHeight(i)
		if data == nil {
			t.Errorf("Missing header at %d", i)
			continue
		}

		t.Logf("Header at %02d : %s", i, data.Hash)
		if !data.Hash.Equal(header.BlockHash()) {
			t.Errorf("Wrong hash at height %d : \ngot  : %s\nwant : %s", i, data.Hash,
				header.BlockHash())
		}

		if height := readBranch.Find(*header.BlockHash()); height != i {
			t.Errorf("Find header at wrong height : got %d, want %d", height, i)
		}
	}
}

func Test_Branches_Consolidate(t *testing.T) {
	ctx := tests.Context()
	store := storage.NewMockStorage()
	genesis := genesisHeader(bitcoin.MainNet)

	rand.Seed(100)

	initialBranch, _ := NewBranch(nil, -1, genesis)
	if initialBranch == nil {
		t.Fatalf("Failed to create initial branch")
	}

	MockHeadersOnBranch(initialBranch, 10)

	if err := initialBranch.Save(ctx, store); err != nil {
		t.Fatalf("Failed to save initial branch : %s", err)
	}

	t.Logf("Initial Branch :")
	t.Logf(initialBranch.String("  "))
	t.Logf(initialBranch.StringHeaderHashes("  "))

	initialBranch.Prune(5)

	previousHeader := initialBranch.AtHeight(8)
	t.Logf("Linking new branch after %s", previousHeader.Hash)

	otherBranchHeader := &wire.BlockHeader{
		Version:   1,
		PrevBlock: previousHeader.Hash,
		Timestamp: previousHeader.Header.Timestamp + 600,
		Bits:      0x1d00ffff,
		Nonce:     rand.Uint32(),
	}
	rand.Read(otherBranchHeader.MerkleRoot[:])

	otherBranch, err := NewBranch(initialBranch, 8, otherBranchHeader)
	if err != nil {
		t.Fatalf("Failed to create other branch : %s", err)
	}
	MockHeadersOnBranch(otherBranch, 11)

	t.Logf("Other Branch :")
	t.Logf(otherBranch.String("  "))
	t.Logf(otherBranch.StringHeaderHashes("  "))

	previousThirdHeader := initialBranch.AtHeight(6)
	thirdBranchHeader := &wire.BlockHeader{
		Version:   1,
		PrevBlock: previousThirdHeader.Hash,
		Timestamp: previousThirdHeader.Header.Timestamp + 600,
		Bits:      0x1d00ffff,
		Nonce:     rand.Uint32(),
	}
	rand.Read(thirdBranchHeader.MerkleRoot[:])

	thirdBranch, err := NewBranch(initialBranch, 6, thirdBranchHeader)
	if err != nil {
		t.Fatalf("Failed to create third branch : %s", err)
	}
	MockHeadersOnBranch(thirdBranch, 5)

	t.Logf("Third Branch :")
	t.Logf(thirdBranch.String("  "))
	t.Logf(thirdBranch.StringHeaderHashes("  "))

	MockHeadersOnBranch(initialBranch, 8)

	consolidateBranch, linkHeight, err := otherBranch.Consolidate(ctx, store, initialBranch)
	if err != nil {
		t.Fatalf("Failed to consolidate branch : %s", err)
	}

	t.Logf("Consolidated :")
	t.Logf(consolidateBranch.String("  "))
	t.Logf(consolidateBranch.StringHeaderHashes("  "))

	if linkHeight != 8 {
		t.Errorf("Wrong parent link height : got %d, want %d", linkHeight, 8)
	}

	if consolidateBranch.Height() != otherBranch.Height() {
		t.Errorf("Wrong consolidated height : got %d, want %d", consolidateBranch.Height(),
			otherBranch.Height())
	}

	if consolidateBranch.PrunedLowestHeight() != initialBranch.PrunedLowestHeight() {
		t.Errorf("Wrong consolidated pruned lowest height : got %d, want %d",
			consolidateBranch.PrunedLowestHeight(), initialBranch.PrunedLowestHeight())
	}

	// Verify headers on consolidated branch
	for i := 6; i <= 8; i++ {
		want := initialBranch.AtHeight(i)
		got := consolidateBranch.AtHeight(i)

		if want == nil {
			t.Fatalf("Missing want header %d", i)
		}
		if got == nil {
			t.Fatalf("Missing got header %d", i)
		}

		if !want.Hash.Equal(&got.Hash) {
			t.Errorf("Wrong hash at height %d : \ngot  : %s\nwant : %s", i, got.Hash, want.Hash)
		}
	}

	for i := 9; i <= 20; i++ {
		want := otherBranch.AtHeight(i)
		got := consolidateBranch.AtHeight(i)

		if want == nil {
			t.Fatalf("Missing want header %d", i)
		}
		if got == nil {
			t.Fatalf("Missing got header %d", i)
		}

		if !want.Hash.Equal(&got.Hash) {
			t.Errorf("Wrong hash at height %d : \ngot  : %s\nwant : %s", i, got.Hash, want.Hash)
		}
	}

	truncateBranch, err := initialBranch.Truncate(ctx, store, consolidateBranch, linkHeight)
	if err != nil {
		t.Fatalf("Failed to truncate branch : %s", err)
	}

	t.Logf("Truncated :")
	t.Logf(truncateBranch.String("  "))
	t.Logf(truncateBranch.StringHeaderHashes("  "))

	if truncateBranch.Height() != initialBranch.Height() {
		t.Errorf("Wrong truncated height : got %d, want %d", truncateBranch.Height(),
			initialBranch.Height())
	}

	if truncateBranch.PrunedLowestHeight() != linkHeight+1 {
		t.Errorf("Wrong consolidated pruned lowest height : got %d, want %d",
			truncateBranch.PrunedLowestHeight(), linkHeight+1)
	}

	if !truncateBranch.firstHeader.PrevBlock.Equal(&previousHeader.Hash) {
		t.Errorf("Wrong truncated previous hash : \ngot  : %s\nwant : %s",
			truncateBranch.firstHeader.PrevBlock, previousHeader.Hash)
	}

	for i := 9; i <= 18; i++ {
		want := initialBranch.AtHeight(i)
		got := truncateBranch.AtHeight(i)

		if want == nil {
			t.Fatalf("Missing want header %d", i)
		}
		if got == nil {
			t.Fatalf("Missing got header %d", i)
		}

		if !want.Hash.Equal(&got.Hash) {
			t.Errorf("Wrong hash at height %d : \ngot  : %s\nwant : %s", i, got.Hash, want.Hash)
		}
	}

	connectBranch, err := thirdBranch.Connect(ctx, store,
		Branches{consolidateBranch, truncateBranch})
	if err != nil {
		t.Fatalf("Failed to connect branch : %s", err)
	}

	t.Logf("Connected :")
	t.Logf(connectBranch.String("  "))
	t.Logf(connectBranch.StringHeaderHashes("  "))

	if connectBranch.parent != consolidateBranch {
		t.Errorf("Wrong parent for connected branch")
	}

	for i := 6; i <= 6; i++ {
		want := consolidateBranch.AtHeight(i)
		got := connectBranch.AtHeight(i)

		if want == nil {
			t.Fatalf("Missing want header %d", i)
		}
		if got == nil {
			t.Fatalf("Missing got header %d", i)
		}

		if !want.Hash.Equal(&got.Hash) {
			t.Errorf("Wrong hash at height %d : \ngot  : %s\nwant : %s", i, got.Hash, want.Hash)
		}
	}

	for i := 7; i <= 12; i++ {
		want := thirdBranch.AtHeight(i)
		got := connectBranch.AtHeight(i)

		if want == nil {
			t.Fatalf("Missing want header %d", i)
		}
		if got == nil {
			t.Fatalf("Missing got header %d", i)
		}

		if !want.Hash.Equal(&got.Hash) {
			t.Errorf("Wrong hash at height %d : \ngot  : %s\nwant : %s", i, got.Hash, want.Hash)
		}
	}
}

func Test_Branches_Trim(t *testing.T) {
	genesis := genesisHeader(bitcoin.MainNet)

	rand.Seed(100)

	initialBranch, _ := NewBranch(nil, -1, genesis)
	if initialBranch == nil {
		t.Fatalf("Failed to create initial branch")
	}

	MockHeadersOnBranch(initialBranch, 10)

	hashes := make([]bitcoin.Hash32, initialBranch.Height()+1)
	for i := range hashes {
		hashes[i] = initialBranch.AtHeight(i).Hash
	}

	previousHeader := initialBranch.AtHeight(8)
	t.Logf("Linking new branch after %s", previousHeader.Hash)

	otherBranchHeader := &wire.BlockHeader{
		Version:   1,
		PrevBlock: previousHeader.Hash,
		Timestamp: previousHeader.Header.Timestamp + 600,
		Bits:      0x1d00ffff,
		Nonce:     rand.Uint32(),
	}
	rand.Read(otherBranchHeader.MerkleRoot[:])

	otherBranch, err := NewBranch(initialBranch, 8, otherBranchHeader)
	if err != nil {
		t.Fatalf("Failed to create other branch : %s", err)
	}
	MockHeadersOnBranch(otherBranch, 11)

	branches := Branches{initialBranch, otherBranch}
	if len(branches) != 2 {
		t.Fatalf("Wrong branch count : got %d, want %d", len(branches), 2)
	}

	branches.Trim(initialBranch, 7)

	if len(branches) != 1 {
		t.Fatalf("Wrong branch count : got %d, want %d", len(branches), 1)
	}

	if initialBranch.Height() != 6 {
		t.Fatalf("Wrong branch height : got %d, want %d", initialBranch.Height(), 6)
	}

	for i := 0; i <= initialBranch.Height(); i++ {
		if !hashes[i].Equal(&initialBranch.AtHeight(i).Hash) {
			t.Errorf("Wrong hash at height %d : \ngot  : %s\nwant : %s", i, hashes[i],
				initialBranch.AtHeight(i).Hash)
		}
	}
}

// Test_Branches_BranchOfBranch tests a consolidation when there is a branch of a branch.
func Test_Branches_BranchOfBranch(t *testing.T) {
	ctx := tests.Context()
	firstBranchSize := 10

	for offset := 0; offset < firstBranchSize; offset++ {
		store := storage.NewMockStorage()
		repo := NewRepository(DefaultConfig(), store)
		repo.DisableDifficulty()

		startTime := uint32(952644136)
		repo.InitializeWithTimeStamp(startTime)
		initialHeaders := MockHeaders(ctx, repo, repo.LastHash(), repo.LastTime(), 15)

		for i, header := range initialHeaders {
			t.Logf("Header %d : %s", 1+i, header.BlockHash())
		}

		firstBranchHeader := initialHeaders[9]
		firstBranchHash := *firstBranchHeader.BlockHash()

		firstBranchHeaders := MockHeaders(ctx, repo, firstBranchHash, firstBranchHeader.Timestamp,
			firstBranchSize)

		for i, header := range firstBranchHeaders {
			t.Logf("Header %d : %s", 11+i, header.BlockHash())
		}

		secondBranchHeader := firstBranchHeaders[offset]
		secondBranchHash := *secondBranchHeader.BlockHash()

		secondBranchHeaders := MockHeaders(ctx, repo, secondBranchHash,
			secondBranchHeader.Timestamp, 8)

		for i, header := range secondBranchHeaders {
			t.Logf("Header %d : %s", 15+i, header.BlockHash())
		}

		for i, branch := range repo.branches {
			t.Logf("Branch %d : %s", i, branch.String(""))
			t.Logf("Hashes : %s", branch.StringHeaderHashes("    "))
		}

		if err := repo.saveMainBranch(ctx); err != nil {
			t.Fatalf("Failed to save main branch : %s", err)
		}

		if err := repo.prune(ctx, 8); err != nil {
			t.Fatalf("Failed to prune repo : %s", err)
		}

		t.Logf("After prune")

		for i, branch := range repo.branches {
			t.Logf("Branch %d : %s", i, branch.String(""))
			t.Logf("Hashes : %s", branch.StringHeaderHashes("    "))
		}

		if err := repo.consolidate(ctx); err != nil {
			t.Fatalf("Failed to consolidate repo : %s", err)
		}

		t.Logf("After consolidate")

		for i, branch := range repo.branches {
			t.Logf("Branch %d : %s", i, branch.String(""))
			t.Logf("Hashes : %s", branch.StringHeaderHashes("    "))
		}
	}
}
