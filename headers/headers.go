package headers

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/big"
	"sort"
	"sync"
	"time"

	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/logger"
	"github.com/tokenized/pkg/merkle_proof"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/pkg/wire"

	"github.com/pkg/errors"
)

const (
	headersPath    = "headers"
	headersPerFile = 1000 // Number of headers stored in each key
	headersVersion = uint8(1)

	invalidHashesPath = "headers/invalid"

	reorgsPath    = "reorgs"
	reorgsVersion = uint8(0)

	// pruneDepth is the depth of the longest chain and branches of that chain that will be kept
	// in memory for processing new headers. New branches deeper than that will not be recognized.
	pruneDepth = 10000
)

var (
	// ErrUnknownHeader means the header isn't known and doesn't link to any known header.
	ErrUnknownHeader = errors.New("Unknown Header")

	// ErrInvalidTarget means the target bits are not correct for the difficulty algorithm.
	ErrInvalidTarget = errors.New("Invalid Target Bits")

	// ErrNotEnoughWork means the target bits are correct but the hash doesn't meet the target.
	ErrNotEnoughWork = errors.New("Not Enough Work")

	// ErrWrongChain means the header is known to belong to another chain (BTC, BCH, ...)
	ErrWrongChain = errors.New("Wrong Chain")

	ErrBeyondMaxBranchDepth = errors.New("Beyond Max Branch Depth")
	ErrHeaderMarkedInvalid  = errors.New("Header Marked Invalid")

	ErrHeightBeyondTip = errors.New("Height Beyond Tip")

	// ErrHeaderNotAvailable means the header has been pruned and is not available.
	ErrHeaderNotAvailable = errors.New("Header Not Available")

	errNeedPOW = errors.New("Need POW")

	endian = binary.LittleEndian
)

// Repository is used for managing header data.
type Repository struct {
	config                 *Config
	store                  storage.Storage
	newHeadersChannels     []chan *wire.BlockHeader
	longest                *Branch
	branches               Branches
	disableDifficulty      bool
	disableSplitProtection bool

	heights map[bitcoin.Hash32]int // Lookup of block height by hash

	// Chain split data
	requiredSplit *Split
	genesisHash   bitcoin.Hash32
	splits        Splits

	invalidHashes []bitcoin.Hash32

	sync.Mutex
}

// NewRepository returns a new Repository.
func NewRepository(config *Config, store storage.Storage) *Repository {
	result := &Repository{
		config:        config,
		store:         store,
		heights:       make(map[bitcoin.Hash32]int),
		invalidHashes: config.InvalidHeaderHashes,
	}

	if config.Network == bitcoin.MainNet {
		// Convert split hashes from hex
		result.splits = make(Splits, len(mainNetSplits))
		for i, h := range mainNetSplits {
			beforeHash, _ := bitcoin.NewHash32FromStr(h.before)
			afterHash, _ := bitcoin.NewHash32FromStr(h.after)
			result.splits[i] = Split{
				Name:       h.name,
				BeforeHash: *beforeHash,
				AfterHash:  *afterHash,
				Height:     h.height,
			}
		}
		sort.Sort(result.splits)

		beforeHash, _ := bitcoin.NewHash32FromStr(mainNetRequiredSplit.before)
		afterHash, _ := bitcoin.NewHash32FromStr(mainNetRequiredSplit.after)
		result.requiredSplit = &Split{
			Name:       mainNetRequiredSplit.name,
			BeforeHash: *beforeHash,
			AfterHash:  *afterHash,
			Height:     mainNetRequiredSplit.height,
		}

	}

	result.genesisHash = *genesisHeader(config.Network).BlockHash()
	result.heights[result.genesisHash] = 0

	return result
}

func (repo *Repository) GetNewHeadersAvailableChannel() <-chan *wire.BlockHeader {
	result := make(chan *wire.BlockHeader, 10000)

	repo.Lock()
	repo.newHeadersChannels = append(repo.newHeadersChannels, result)
	repo.Unlock()

	return result
}

func (repo *Repository) Height() int {
	repo.Lock()
	defer repo.Unlock()

	return repo.longest.Height()
}

func (repo *Repository) LastHash() bitcoin.Hash32 {
	repo.Lock()
	defer repo.Unlock()

	return repo.longest.Last().Hash
}

func (repo *Repository) LastTime() uint32 {
	repo.Lock()
	defer repo.Unlock()

	return repo.longest.Last().Header.Timestamp
}

func (repo *Repository) AccumulatedWork() *big.Int {
	repo.Lock()
	defer repo.Unlock()

	return repo.longest.Last().AccumulatedWork
}

// HashHeight returns the block height of the specified hash.
func (repo *Repository) HashHeight(hash bitcoin.Hash32) int {
	repo.Lock()
	defer repo.Unlock()

	_, height := repo.branches.Find(hash)
	if height != -1 {
		return height
	}

	// Lookup in larger map
	if result, exists := repo.heights[hash]; exists {
		return result
	}

	return -1
}

func (repo *Repository) PreviousHash(hash bitcoin.Hash32) (*bitcoin.Hash32, int) {
	repo.Lock()
	defer repo.Unlock()

	branch, height := repo.branches.Find(hash)
	if height == -1 {
		return nil, -1
	}

	at := branch.AtHeight(height - 1)
	if at == nil {
		return nil, -1
	}

	return &at.Hash, height - 1
}

func (repo *Repository) Stop(ctx context.Context) {
	repo.Lock()
	defer repo.Unlock()

	for _, channel := range repo.newHeadersChannels {
		close(channel)
		repo.newHeadersChannels = nil
	}
}

// GetLocatorHashes is used to populate initial P2P header requests. They are designed to quickly
// identify which chain the other node is on.
// This inserts split hashes at the appropriate heights so that if a node is on one of those
// branches then they will respond with the header immediately after the split, which makes it
// easier to identify when a node is on a different chain. It adds them after "max" hashes if they
// didn't fit in anywhere else.
func (repo *Repository) GetLocatorHashes(ctx context.Context, max int) ([]bitcoin.Hash32, error) {
	repo.Lock()
	defer repo.Unlock()

	accumulatedHeightHashes := repo.longest.GetLocatorHashes(repo.splits, 5, max)
	for _, branch := range repo.branches {
		if branch == repo.longest {
			continue
		}

		accumulatedHeightHashes = append(accumulatedHeightHashes, &HeightHash{
			Height: branch.PrunedLowestHeight(),
			Hash:   branch.AtHeight(branch.PrunedLowestHeight()).Hash,
		})
	}

	sort.Sort(accumulatedHeightHashes)

	result := make([]bitcoin.Hash32, len(accumulatedHeightHashes))
	for i := range result {
		result[i] = accumulatedHeightHashes[i].Hash
	}

	return removeDuplicateHashes(result), nil
}

func (repo *Repository) GetVerifyOnlyLocatorHashes(ctx context.Context) ([]bitcoin.Hash32, error) {
	repo.Lock()
	defer repo.Unlock()

	var accumulatedHeightHashes HeightHashes
	if repo.requiredSplit != nil {
		accumulatedHeightHashes = append(accumulatedHeightHashes, &HeightHash{
			Height: repo.requiredSplit.Height - 1,
			Hash:   repo.requiredSplit.BeforeHash,
		})
	}

	for _, split := range repo.splits {
		accumulatedHeightHashes = append(accumulatedHeightHashes, &HeightHash{
			Height: split.Height - 1,
			Hash:   split.BeforeHash,
		})
	}

	sort.Sort(accumulatedHeightHashes)

	result := make([]bitcoin.Hash32, len(accumulatedHeightHashes))
	for i := range result {
		result[i] = accumulatedHeightHashes[i].Hash
	}

	return removeDuplicateHashes(result), nil
}

func removeDuplicateHashes(hashes []bitcoin.Hash32) []bitcoin.Hash32 {
	result := make([]bitcoin.Hash32, 0, len(hashes))
	var previousHash bitcoin.Hash32
	for i, hash := range hashes {
		if i != 0 && previousHash.Equal(&hash) {
			continue
		}
		result = append(result, hash)
	}

	return result
}

// VerifyHeader verifies that a header was returned for the correct chain.
// "GetVerifyOnlyLocatorHashes" was used to request, so they should respond starting with a split
// header.
func (repo *Repository) VerifyHeader(ctx context.Context, header *wire.BlockHeader) error {
	hash := header.BlockHash()

	repo.Lock()
	defer repo.Unlock()

	if repo.requiredSplit != nil {
		if repo.requiredSplit.AfterHash.Equal(hash) {
			return nil
		}
	}

	for _, split := range repo.splits {
		if split.AfterHash.Equal(hash) {
			return errors.Wrap(ErrWrongChain, split.Name)
		}
	}

	if repo.genesisHash.Equal(&header.PrevBlock) {
		return errors.New("Header after genesis")
	}

	return ErrUnknownHeader
}

// ProcessHeader adds a header to the header repo if it is valid. It will return no error if we
// already have the header. It will return an error if the header is invalid, unknown, or on the
// wrong chain.
func (repo *Repository) ProcessHeader(ctx context.Context, header *wire.BlockHeader) error {
	// defer logger.Elapsed(ctx, time.Now(), "Process header")
	repo.Lock()
	defer repo.Unlock()

	if !repo.disableDifficulty && !header.WorkIsValid() {
		return ErrNotEnoughWork
	}

	hash := *header.BlockHash()
	previousBranch, previousHeight := repo.branches.Find(header.PrevBlock)
	if previousBranch == nil {
		for _, split := range repo.splits {
			if split.AfterHash.Equal(&hash) {
				logger.InfoWithFields(ctx, []logger.Field{
					logger.String("chain", split.Name),
					logger.Stringer("block_hash", hash),
				}, "Header from another chain")
				return errors.Wrap(ErrWrongChain, split.Name)
			}
		}

		if repo.genesisHash.Equal(&header.PrevBlock) {
			logger.InfoWithFields(ctx, []logger.Field{
				logger.Stringer("block_hash", hash),
			}, "Header after genesis")
			return errors.Wrap(ErrWrongChain, "after genesis")
		}

		return ErrUnknownHeader
	}

	height := previousHeight + 1
	containingBranch, _ := repo.branches.Find(hash)
	if containingBranch != nil {
		return nil // already have this header
	}

	if !repo.disableSplitProtection {
		// Check for split hashes
		for _, split := range repo.splits {
			if split.Height == height && split.AfterHash.Equal(&hash) {
				return errors.Wrap(ErrWrongChain, split.Name)
			}
		}

		// Ensure we are on the correct chain
		if repo.requiredSplit != nil && height == repo.requiredSplit.Height {
			if repo.requiredSplit.AfterHash.Equal(&hash) {
				logger.InfoWithFields(ctx, []logger.Field{
					logger.Stringer("block_hash", hash),
					logger.Int("block_height", height),
				}, "Verified chain header")
			} else {
				return errors.Wrap(ErrWrongChain, "wrong header at split height")
			}
		}
	}

	// Only verify after BCH split and difficulty is not disabled
	if height >= 556767 && !repo.disableDifficulty {
		target, err := previousBranch.Target(ctx, height)
		if err != nil {
			return errors.Wrap(err, "calculate target")
		}

		bits := bitcoin.ConvertToBits(target, bitcoin.MaxBits)
		if bits != header.Bits {
			return errors.Wrapf(ErrInvalidTarget,
				"height %d, calculated : 0x%08x, header 0x%08x", height, bits, header.Bits)
		}
	}

	for _, invalidHash := range repo.invalidHashes {
		if invalidHash.Equal(&hash) {
			return errors.Wrap(ErrHeaderMarkedInvalid, hash.String())
		}
	}

	last := previousBranch.Last()
	if !last.Hash.Equal(&header.PrevBlock) {
		// There is already a header at this height in this branch so create a new branch
		depth := repo.longest.Height() - previousHeight
		if depth > repo.config.MaxBranchDepth {
			logger.InfoWithFields(ctx, []logger.Field{
				logger.Stringer("previous_block_hash", header.PrevBlock),
				logger.Int("block_height", previousHeight+1),
				logger.Stringer("block_hash", header.BlockHash()),
			}, "New header branch rejected for over max branch depth")

			return errors.Wrapf(ErrBeyondMaxBranchDepth, "depth %d, max %d", depth,
				repo.config.MaxBranchDepth)
		}

		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("previous_block_hash", header.PrevBlock),
			logger.Int("block_height", previousHeight+1),
			logger.Stringer("block_hash", header.BlockHash()),
		}, "Starting new header branch")

		newBranch, err := NewBranch(previousBranch, previousHeight, header)
		if err != nil {
			return errors.Wrap(err, "new branch")
		}
		repo.branches = append(repo.branches, newBranch)
		repo.heights[*header.BlockHash()] = previousHeight + 1

		longest := repo.branches.Longest()
		if repo.longest != longest {
			logger.InfoWithFields(ctx, []logger.Field{
				logger.Stringer("previous_block_hash", header.PrevBlock),
				logger.Int("block_height", longest.Height()),
				logger.Stringer("block_hash", longest.Last().Hash),
				logger.String("previous_work", repo.longest.Last().AccumulatedWork.Text(16)),
				logger.String("new_work", longest.Last().AccumulatedWork.Text(16)),
			}, "New longest header branch")

			if err := repo.sendBranchUpdate(longest, repo.longest); err != nil {
				return errors.Wrap(err, "send branch update")
			}

			repo.longest = longest
		}

		return nil
	}

	if !previousBranch.Add(header) {
		return errors.New("Failed to add header to branch")
	}
	repo.heights[*header.BlockHash()] = height

	headersSent := false
	if previousBranch != repo.longest {
		longest := repo.branches.Longest()
		if repo.longest != longest {
			logger.InfoWithFields(ctx, []logger.Field{
				logger.Stringer("intersect_block_hash", repo.longest.IntersectHash(longest)),
				logger.Int("block_height", longest.Height()),
				logger.Stringer("block_hash", longest.Last().Hash),
				logger.String("previous_work", repo.longest.Last().AccumulatedWork.Text(16)),
				logger.String("new_work", longest.Last().AccumulatedWork.Text(16)),
			}, "New longest header branch")

			if err := repo.sendBranchUpdate(longest, repo.longest); err != nil {
				return errors.Wrap(err, "send branch update")
			}

			headersSent = true
			repo.longest = longest
		}
	}

	if previousBranch == repo.longest {
		currentTimestamp := uint32(time.Now().Unix())
		isRecent := currentTimestamp < header.Timestamp || currentTimestamp-header.Timestamp < 3600

		if isRecent || previousBranch.Height()%1000 == 0 {
			logger.InfoWithFields(ctx, []logger.Field{
				logger.Stringer("previous_block_hash", header.PrevBlock),
				logger.Int("block_height", previousBranch.Height()),
				logger.Stringer("block_hash", previousBranch.Last().Hash),
			}, "New longest chain tip")
		}

		if previousBranch.Height()%10000 == 0 {
			if err := repo.clean(ctx); err != nil {
				logger.Error(ctx, "Failed to clean headers : %s", err)
			}
		}

		if !headersSent {
			for _, channel := range repo.newHeadersChannels {
				channel <- header
			}
		}
	} else {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("intersect_block_hash", repo.longest.IntersectHash(previousBranch)),
			logger.Stringer("previous_block_hash", header.PrevBlock),
			logger.Int("block_height", previousBranch.Height()),
			logger.Stringer("block_hash", previousBranch.Last().Hash),
		}, "New branch chain tip")
	}

	return nil
}

func (repo *Repository) sendBranchUpdate(branch, previousLongest *Branch) error {
	intersect := branch.IntersectHash(previousLongest)
	if intersect == nil {
		return errors.New("Intersect not found")
	}

	branchHeight := branch.Find(*intersect)
	if branchHeight == -1 {
		return errors.New("Intersect missing")
	}
	height := branchHeight + 1
	latestHeight := branch.Height()

	for ; height <= latestHeight; height++ {
		item := branch.AtHeight(height)
		if item == nil {
			return errors.New("Height Unavailable")
		}

		for _, channel := range repo.newHeadersChannels {
			channel <- item.Header
		}
	}

	return nil
}

// VerifyMerkleProof verifies that the merkle proof connects to the valid chain of headers and that
// the calculated merkle root matches the header. It returns the block height of the header that
// this proof is linked to and true if it is in the longest POW chain.
//
// TODO We might need to load older headers than are in memory to verify older merkle proofs --ce
//
func (repo *Repository) VerifyMerkleProof(ctx context.Context,
	proof *merkle_proof.MerkleProof) (int, bool, error) {

	var blockHeight int
	isLongest := false
	if proof.BlockHeader != nil {
		hash := *proof.BlockHeader.BlockHash()

		repo.Lock()

		branch, height := repo.branches.Find(hash)
		if branch == nil {
			repo.Unlock()
			return -1, false, errors.Wrap(ErrUnknownHeader, hash.String())
		}
		blockHeight = height
		isLongest = branch == repo.longest

		repo.Unlock()
	} else if proof.BlockHash != nil {
		repo.Lock()

		branch, height := repo.branches.Find(*proof.BlockHash)
		if branch == nil {
			repo.Unlock()
			return -1, false, errors.Wrap(ErrUnknownHeader, proof.BlockHash.String())
		}
		blockHeight = height
		isLongest = branch == repo.longest
		data := branch.AtHeight(height)
		if data == nil {
			repo.Unlock()
			return -1, false, errors.Wrap(ErrHeaderNotAvailable, proof.BlockHash.String())
		}
		proof.BlockHeader = data.Header

		repo.Unlock()
	} else {
		return -1, false, merkle_proof.ErrNotVerifiable
	}

	if err := proof.Verify(); err != nil {
		return -1, false, errors.Wrap(err, "merkle proof")
	}

	return blockHeight, isLongest, nil
}

// GetHeader returns the header with the specified hash with its block height and whether it is
// in the most proof of work chain.
func (repo *Repository) GetHeader(ctx context.Context,
	hash bitcoin.Hash32) (*wire.BlockHeader, int, bool, error) {
	repo.Lock()
	defer repo.Unlock()

	branch, height := repo.branches.Find(hash)
	if branch != nil {
		data := branch.AtHeight(height)
		if data == nil {
			return nil, -1, false, ErrHeaderNotAvailable
		}

		return data.Header, height, branch == repo.longest, nil
	}

	// Lookup in larger map
	if height, exists := repo.heights[hash]; exists {
		header, err := repo.header(ctx, height)
		if err != nil {
			return nil, -1, false, err
		}

		return header, height, true, nil
	}

	return nil, -1, false, ErrUnknownHeader
}

// Header returns the header at the specified height on the longest POW chain.
func (repo *Repository) Header(ctx context.Context, height int) (*wire.BlockHeader, error) {
	repo.Lock()
	defer repo.Unlock()

	return repo.header(ctx, height)
}

func (repo *Repository) header(ctx context.Context, height int) (*wire.BlockHeader, error) {
	if height > repo.longest.Height() {
		return nil, ErrHeightBeyondTip
	}

	data := repo.longest.AtHeight(height)
	if data != nil {
		return data.Header, nil
	}

	file := height / headersPerFile
	headersData, err := repo.getData(ctx, file)
	if err != nil {
		return nil, errors.Wrap(err, "get data")
	}

	offset := height - (file * headersPerFile)
	if offset >= len(headersData) {
		return nil, errors.New("File missing data")
	}

	return headersData[offset].Header, nil
}

func (repo *Repository) Hash(ctx context.Context, height int) (*bitcoin.Hash32, error) {
	repo.Lock()
	defer repo.Unlock()

	if height > repo.longest.Height() {
		return nil, ErrHeightBeyondTip
	}

	data := repo.longest.AtHeight(height)
	if data != nil {
		return &data.Hash, nil
	}

	file := height / headersPerFile
	headersData, err := repo.getData(ctx, file)
	if err != nil {
		return nil, errors.Wrap(err, "get data")
	}

	offset := height - (file * headersPerFile)
	if offset >= len(headersData) {
		return nil, errors.New("File missing data")
	}

	return &headersData[offset].Hash, nil
}

func (repo *Repository) GetHeaders(ctx context.Context,
	startHeight, maxCount int) ([]*wire.BlockHeader, error) {
	repo.Lock()
	defer repo.Unlock()

	result := make([]*wire.BlockHeader, 0, maxCount)
	headersFile := -1
	var headersData []*HeaderData
	for height := startHeight; ; height++ {
		at := repo.longest.AtHeight(height)
		if at != nil {
			result = append(result, at.Header)
			if len(result) == maxCount {
				break
			}
			continue
		}

		wantFile := height / headersPerFile
		if headersFile != wantFile {
			data, err := repo.getData(ctx, wantFile)
			if err != nil {
				return nil, errors.Wrap(err, "get data")
			}
			headersData = data
			headersFile = wantFile
		}

		offset := height - (headersFile * headersPerFile)
		if offset >= len(headersData) {
			break
		}

		result = append(result, headersData[offset].Header)
		if len(result) == maxCount {
			break
		}
	}

	return result, nil
}

func (repo *Repository) getData(ctx context.Context, file int) ([]*HeaderData, error) {
	path := headersFilePath(file)
	data, err := repo.store.Read(ctx, path)
	if err != nil {
		return nil, errors.Wrap(err, "read")
	}
	buf := bytes.NewReader(data)

	var version uint8
	if err := binary.Read(buf, endian, &version); err != nil {
		return nil, errors.Wrap(err, "version")
	}
	if version != 1 {
		return nil, fmt.Errorf("Wrong version : %d", version)
	}

	result := make([]*HeaderData, buf.Len()/headerDataSerializeSize)
	for i := range result {
		headerData := &HeaderData{}
		if err := headerData.Deserialize(buf); err != nil {
			return nil, errors.Wrap(err, "deserialize")
		}

		result[i] = headerData
	}

	return result, nil
}

func (repo *Repository) MarkHeaderInvalid(ctx context.Context, hash bitcoin.Hash32) error {
	repo.Lock()
	defer repo.Unlock()

	for _, invalidHash := range repo.invalidHashes {
		if invalidHash.Equal(&hash) {
			return nil // already marked
		}
	}

	repo.invalidHashes = append(repo.invalidHashes, hash)
	if err := saveInvalidHashes(ctx, repo.store, repo.invalidHashes); err != nil {
		return errors.Wrap(err, "save invalid hashes")
	}

	// Check if hash was previously accepted
	branch, height := repo.branches.Find(hash)
	if branch != nil {
		return nil // not found
	}

	if err := repo.branches.Trim(branch, height); err != nil {
		return errors.Wrap(err, "trim")
	}

	longest := repo.branches.Longest()
	if repo.longest != longest {
		// TODO Trigger reorg processing --ce
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Int("block_height", longest.Height()),
			logger.Stringer("block_hash", longest.Last().Hash),
		}, "New longest header branch")
	}
	repo.longest = longest

	return nil
}

func (repo *Repository) MarkHeaderNotInvalid(ctx context.Context, hash bitcoin.Hash32) error {
	repo.Lock()
	defer repo.Unlock()

	found := false
	for i, invalidHash := range repo.invalidHashes {
		if invalidHash.Equal(&hash) {
			found = true
			repo.invalidHashes = append(repo.invalidHashes[:i], repo.invalidHashes[i+1:]...)
			break
		}
	}

	if !found {
		return nil // wasn't marked invalid
	}

	if err := saveInvalidHashes(ctx, repo.store, repo.invalidHashes); err != nil {
		return errors.Wrap(err, "save invalid hashes")
	}

	return nil
}

// Clean reorganizes the branches. It makes the longest branch a single branch going back to zero.
// It also prunes old headers from memory.
func (repo *Repository) Clean(ctx context.Context) error {
	repo.Lock()
	defer repo.Unlock()

	return repo.clean(ctx)
}

func (repo *Repository) clean(ctx context.Context) error {
	start := time.Now()

	// Consolidate longest branch with oldest branch so the main chain is in one branch.
	if err := repo.consolidate(ctx); err != nil {
		return errors.Wrap(err, "consolidate")
	}

	// Write oldest branch headers to main header set and prune.
	if err := repo.saveMainBranch(ctx); err != nil {
		return errors.Wrap(err, "save main branches")
	}

	// Prune old headers from memory.
	if err := repo.prune(ctx, pruneDepth); err != nil {
		return errors.Wrap(err, "prune")
	}

	if err := saveInvalidHashes(ctx, repo.store, repo.invalidHashes); err != nil {
		return errors.Wrap(err, "invalid hashes")
	}

	logger.ElapsedWithFields(ctx, start, []logger.Field{
		logger.Int("block_height", repo.longest.Height()),
		logger.Stringer("block_hash", repo.longest.Last().Hash),
	}, "Cleaned and saved headers")

	return nil
}

func (repo *Repository) consolidate(ctx context.Context) error {
	if repo.longest.parentHeight != -1 {
		// Consolidate longest branch into the branch that goes back to oldest
		var oldestBranch *Branch
		for _, branch := range repo.branches {
			if branch.parentHeight == -1 {
				oldestBranch = branch
				break
			}
		}

		if oldestBranch == nil {
			return errors.New("Missing oldest branch")
		}

		longestBranch := repo.longest

		// Convert longest branch into oldest branch.
		newMainBranch, linkHeight, err := longestBranch.Consolidate(ctx, repo.store, oldestBranch)
		if err != nil {
			return errors.Wrap(err, "derive")
		}
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Int("from_height", newMainBranch.PrunedLowestHeight()),
			logger.Int("to_height", newMainBranch.Height()),
			logger.String("accumulated_work", newMainBranch.Last().AccumulatedWork.Text(16)),
			logger.Stringer("tip", newMainBranch.Last().Hash),
		}, "Derived new main header branch")

		newBranches := Branches{newMainBranch}

		// Reconnect previously oldest branch to the new main branch.
		newOldestBranch, err := oldestBranch.Truncate(ctx, repo.store, newMainBranch, linkHeight)
		if err != nil {
			return errors.Wrap(err, "truncate previous oldest to main")
		}

		newBranches = append(newBranches, newOldestBranch)

		// Sort by parent height so they can be properly connected to the new main branch.
		sort.Sort(repo.branches)

		// Reconnect other branches to the new main branch. This should result in the previously
		// main branch being made a child to this branch.
		for _, branch := range repo.branches {
			if branch == oldestBranch || branch == longestBranch {
				continue // already replaced by new branches
			}

			newBranch, err := branch.Connect(ctx, repo.store, newBranches)
			if err != nil {
				return errors.Wrap(err, "connect to main")
			}
			newBranches = append(newBranches, newBranch)
		}

		repo.branches = newBranches
		repo.longest = newMainBranch
	}

	return nil
}

// saveMainBranch saves all of the main branch back to the genesis header.
func (repo *Repository) saveMainBranch(ctx context.Context) error {
	mainBranch := repo.longest
	height := mainBranch.PrunedLowestHeight()

	file := height / headersPerFile
	fileHeight := file * headersPerFile
	nextFileHeight := fileHeight + headersPerFile
	path := headersFilePath(file)
	currentFileByteOffset := ((height - fileHeight) * headerDataSerializeSize)
	buf := &bytes.Buffer{}

	if currentFileByteOffset > 0 {
		data, err := repo.store.Read(ctx, path)
		if err != nil {
			return errors.Wrapf(err, "read: %s", path)
		}
		if data[0] != 1 {
			return fmt.Errorf("Wrong version %d: %s", data[0], path)
		}

		if _, err := buf.Write(data[:currentFileByteOffset+1]); err != nil {
			return errors.Wrap(err, "write first file start")
		}
	} else {
		if err := binary.Write(buf, endian, headersVersion); err != nil {
			return errors.Wrap(err, "version")
		}
	}

	for _, header := range mainBranch.headers {
		if err := header.Serialize(buf); err != nil {
			return errors.Wrapf(err, "write header %d", height)
		}
		height++

		if height == nextFileHeight {
			if err := repo.store.Write(ctx, path, buf.Bytes(), nil); err != nil {
				return errors.Wrapf(err, "write: %s", path)
			}

			file++
			fileHeight = nextFileHeight
			nextFileHeight += headersPerFile
			path = headersFilePath(file)
			buf = &bytes.Buffer{}
			if err := binary.Write(buf, endian, headersVersion); err != nil {
				return errors.Wrap(err, "version")
			}
		}
	}

	if buf.Len() > 0 {
		if err := repo.store.Write(ctx, path, buf.Bytes(), nil); err != nil {
			return errors.Wrapf(err, "write: %s", path)
		}
		file++
		fileHeight = nextFileHeight
		path = headersFilePath(file)
	}

	// Delete next file if it exists to prevent loading it later. In case the chain has less headers
	// now.
	if err := repo.store.Remove(ctx, path); err != nil && errors.Cause(err) != storage.ErrNotFound {
		return errors.Wrap(err, "remove after last")
	}

	return nil
}

func headersFilePath(index int) string {
	return fmt.Sprintf("%s/%08x", headersPath, index)
}

// saveBranches saves all of the branch data.
func (repo *Repository) saveBranches(ctx context.Context) error {
	indexBuf := &bytes.Buffer{}
	if err := binary.Write(indexBuf, endian, uint32(len(repo.branches))); err != nil {
		return errors.Wrap(err, "index count")
	}
	for _, branch := range repo.branches {
		if err := branch.Save(ctx, repo.store); err != nil {
			return errors.Wrapf(err, "branch %s", branch.Name())
		}

		if _, err := indexBuf.Write(branch.firstHeader.BlockHash()[:]); err != nil {
			return errors.Wrap(err, "index entry")
		}
	}

	// Save index of active branches
	indexPath := fmt.Sprintf("%s/index", branchPath)
	if err := repo.store.Write(ctx, indexPath, indexBuf.Bytes(), nil); err != nil {
		return errors.Wrap(err, "write index")
	}

	return nil
}

func (repo *Repository) prune(ctx context.Context, depth int) error {
	// Prune branches
	height := repo.longest.Height()
	pruneHeight := height - depth
	logger.InfoWithFields(ctx, []logger.Field{
		logger.Int("block_height", height),
		logger.Stringer("latest_block_hash", repo.longest.Last().Hash),
		logger.Int("prune_height", pruneHeight),
	}, "Pruning headers")

	var newBranches Branches
	for _, branch := range repo.branches {
		if err := branch.Save(ctx, repo.store); err != nil {
			return errors.Wrap(err, "save branch")
		}

		if branch.Height() < pruneHeight {
			// Remove from memory
			continue
		}

		if branch.PrunedLowestHeight() < pruneHeight {
			// Prune the oldest data
			branch.Prune(pruneHeight - branch.PrunedLowestHeight())
		}

		newBranches = append(newBranches, branch)
	}
	repo.branches = newBranches

	return nil
}

func (repo *Repository) Save(ctx context.Context) error {
	repo.Lock()
	defer repo.Unlock()

	if err := repo.saveMainBranch(ctx); err != nil {
		return errors.Wrap(err, "main branch")
	}

	if err := repo.saveBranches(ctx); err != nil {
		return errors.Wrap(err, "branches")
	}

	if err := saveInvalidHashes(ctx, repo.store, repo.invalidHashes); err != nil {
		return errors.Wrap(err, "invalid hashes")
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Int("block_height", repo.longest.Height()),
		logger.Stringer("block_hash", repo.longest.Last().Hash),
	}, "Saved headers")

	return nil
}

func (repo *Repository) Load(ctx context.Context) error {
	repo.Lock()
	defer repo.Unlock()

	return repo.load(ctx, pruneDepth)
}

func (repo *Repository) load(ctx context.Context, depth int) error {
	invalidHashes, err := loadInvalidHashes(ctx, repo.store)
	if err != nil {
		return errors.Wrap(err, "invalid hashes")
	}
	repo.invalidHashes = invalidHashes

	for _, hash := range repo.config.InvalidHeaderHashes {
		found := false
		for _, invalidHash := range repo.invalidHashes {
			if invalidHash.Equal(&hash) {
				found = true
				break
			}
		}

		if !found {
			repo.invalidHashes = append(repo.invalidHashes, hash)
		}
	}

	indexPath := fmt.Sprintf("%s/index", branchPath)
	indexData, err := repo.store.Read(ctx, indexPath)
	if err != nil {
		if errors.Cause(err) == storage.ErrNotFound {
			logger.Info(ctx, "No header branches found to load")
			return repo.migrate(ctx)
		}
		return errors.Wrap(err, "read index")
	}
	indexBuf := bytes.NewReader(indexData)

	var indexCount uint32
	if err := binary.Read(indexBuf, endian, &indexCount); err != nil {
		return errors.Wrap(err, "index count")
	}

	if indexCount == 0 {
		return errors.New("No branches to load")
	}

	repo.branches = make(Branches, 0, indexCount)
	pruneHeight := -1
	for i := uint32(0); i < indexCount; i++ {
		hash := &bitcoin.Hash32{}
		if err := hash.Deserialize(indexBuf); err != nil {
			return errors.Wrapf(err, "index %d", i)
		}

		branch, err := LoadBranch(ctx, repo.store, *hash)
		if err != nil {
			return errors.Wrapf(err, "branch %s", hash)
		}

		if pruneHeight == -1 { // use height of first branch since it is the longest
			pruneHeight = branch.Height() - depth
		}

		if branch.Height() < pruneHeight {
			logger.InfoWithFields(ctx, []logger.Field{
				logger.String("branch", branch.Name()),
			}, "Pruning branch")
			continue
		}

		if branch.PrunedLowestHeight() <= pruneHeight {
			branch.Prune(pruneHeight - branch.PrunedLowestHeight())
		}

		repo.branches = append(repo.branches, branch)
		repo.loadBranchHashHeights(ctx, branch)
	}

	if len(repo.branches) == 0 {
		return errors.New("No branches loaded")
	}
	repo.longest = repo.branches.Longest()

	// Connect branches to parents
	sort.Sort(repo.branches)
	for _, branch := range repo.branches {
		if branch.parentHeight == -1 {
			continue
		}
		if err := branch.Link(repo.branches); err != nil {
			return errors.Wrapf(err, "link branch %s", branch.Name())
		}
	}

	if err := repo.loadHistoricalHashHeights(ctx); err != nil {
		return errors.Wrap(err, "historical heights")
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Int("block_height", repo.longest.Height()),
		logger.Stringer("block_hash", repo.longest.Last().Hash),
	}, "Loaded headers")

	return nil
}

func (repo *Repository) loadBranchHashHeights(ctx context.Context, branch *Branch) {
	height := branch.parentHeight + 1
	for _, headerData := range branch.headers {
		repo.heights[headerData.Hash] = height
		height++
	}
}

func (repo *Repository) loadHistoricalHashHeights(ctx context.Context) error {
	mainBranch := repo.longest
	height := mainBranch.PrunedLowestHeight()

	count := 0
	file := height / headersPerFile
	fileHeight := file * headersPerFile
	currentFileByteOffset := ((height - fileHeight) * headerDataSerializeSize)

	if currentFileByteOffset == 0 {
		if file == 0 {
			logger.Info(ctx, "No historical header hash heights to load")
			return nil
		}
		file--
	}

	for {
		fileHeight = file * headersPerFile
		path := headersFilePath(file)
		data, err := repo.store.Read(ctx, path)
		if err != nil {
			return errors.Wrapf(err, "read: %s", path)
		}

		buf := bytes.NewReader(data)

		var version uint8
		if err := binary.Read(buf, endian, &version); err != nil {
			return errors.Wrap(err, "version")
		}

		if version != 1 {
			return fmt.Errorf("Unknown version : %d", version)
		}

		currentHeight := fileHeight
		headerData := &HeaderData{}
		for buf.Len() >= headerDataSerializeSize {
			if err := headerData.Deserialize(buf); err != nil {
				return errors.Wrap(err, "deserialize")
			}

			repo.heights[headerData.Hash] = currentHeight
			currentHeight++
			count++
		}

		if file == 0 {
			logger.InfoWithFields(ctx, []logger.Field{
				logger.Int("hash_count", count),
			}, "Loaded historical header hash heights")
			return nil
		}

		file--
	}
}

// migrate creates a default branch from the old header data and converts the main branch header
// files to the new format containing accumulated work. This supports migration from before branches
// were supported.
func (repo *Repository) migrate(ctx context.Context) error {
	var branch *Branch

	file := 0
	height := 0
	done := false

	for {
		headers, err := getOldData(ctx, repo.store, headersFilePath(file))
		if err != nil {
			logger.Info(ctx, "Could not get old header data for height %d : %s", height, err)
			break
		}

		logger.InfoWithFields(ctx, []logger.Field{
			logger.Int("file", file),
		}, "Migrating old file")

		for _, header := range headers {
			if branch == nil {
				branch, _ = NewBranch(nil, -1, header)
				repo.longest = branch
				repo.branches = Branches{repo.longest}
			} else if !branch.Add(header) {
				logger.InfoWithFields(ctx, []logger.Field{
					logger.Stringer("previous_block_hash", header.PrevBlock),
					logger.Stringer("block_hash", header.BlockHash()),
					logger.Int("block_height", height),
				}, "Could not add old header")
				done = true
				break
			}
			height++
		}

		if err := repo.saveNewFile(ctx, file, branch); err != nil {
			return errors.Wrapf(err, "save new file %d", file)
		}

		if done {
			break
		}

		if len(headers) != headersPerFile {
			logger.InfoWithFields(ctx, []logger.Field{
				logger.Int("file", file),
				logger.Int("header_count", len(headers)),
			}, "Header file not full")
			break
		}

		file++
	}

	if branch == nil {
		logger.Info(ctx, "Initializing headers with genesis")
		return repo.initializeWithGenesis()
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("latest_block_hash", branch.Last().Hash),
		logger.Int("latest_block_height", branch.Height()),
	}, "Migrated headers")
	return nil
}

func getOldData(ctx context.Context, store storage.Storage,
	path string) ([]*wire.BlockHeader, error) {

	data, err := store.Read(ctx, path)
	if err != nil {
		return nil, errors.Wrap(err, "read")
	}
	buf := bytes.NewReader(data)

	var version uint8
	if err := binary.Read(buf, endian, &version); err != nil {
		return nil, errors.Wrap(err, "version")
	}

	if version != 0 {
		return nil, fmt.Errorf("Unknown version : %d", version)
	}

	result := make([]*wire.BlockHeader, buf.Len()/80)
	for i := range result {
		headerData := &wire.BlockHeader{}
		if err := headerData.Deserialize(buf); err != nil {
			return nil, errors.Wrap(err, "deserialize")
		}

		result[i] = headerData
	}

	return result, nil
}

func (repo *Repository) saveNewFile(ctx context.Context, file int, branch *Branch) error {
	logger.InfoWithFields(ctx, []logger.Field{
		logger.Int("file", file),
	}, "Saving new file")

	fileHeight := file * headersPerFile
	nextFileHeight := fileHeight + headersPerFile
	path := headersFilePath(file)

	lastHeight := branch.Height()
	if lastHeight < nextFileHeight {
		nextFileHeight = lastHeight + 1
	}

	buf := &bytes.Buffer{}
	if err := binary.Write(buf, endian, headersVersion); err != nil {
		return errors.Wrap(err, "version")
	}

	for height := fileHeight; height < nextFileHeight; height++ {
		header := branch.AtHeight(height)
		if header == nil {
			return fmt.Errorf("Could not fetch header %d", height)
		}

		if err := header.Serialize(buf); err != nil {
			return errors.Wrapf(err, "write header %d", height)
		}
	}

	if err := repo.store.Write(ctx, path, buf.Bytes(), nil); err != nil {
		return errors.Wrapf(err, "write : %s", path)
	}

	return nil
}

func loadInvalidHashes(ctx context.Context, store storage.Storage) ([]bitcoin.Hash32, error) {
	data, err := store.Read(ctx, invalidHashesPath)
	if err != nil {
		if errors.Cause(err) == storage.ErrNotFound {
			return nil, nil
		}
		return nil, errors.Wrap(err, "read")
	}
	buf := bytes.NewReader(data)

	var count uint32
	if err := binary.Read(buf, endian, &count); err != nil {
		return nil, errors.Wrap(err, "count")
	}

	result := make([]bitcoin.Hash32, count)
	for i := range result {
		hash := &bitcoin.Hash32{}
		if err := hash.Deserialize(buf); err != nil {
			return nil, errors.Wrapf(err, "hash %d", i)
		}

		result[i] = *hash
	}

	return result, nil
}

func saveInvalidHashes(ctx context.Context, store storage.Storage, hashes []bitcoin.Hash32) error {
	buf := &bytes.Buffer{}
	if err := binary.Write(buf, endian, uint32(len(hashes))); err != nil {
		return errors.Wrap(err, "count")
	}

	for i, hash := range hashes {
		if err := hash.Serialize(buf); err != nil {
			return errors.Wrapf(err, "hash %d", i)
		}
	}

	if err := store.Write(ctx, invalidHashesPath, buf.Bytes(), nil); err != nil {
		return errors.Wrap(err, "write")
	}

	return nil
}

// Initialize with the genesis header.
func (repo *Repository) InitializeWithGenesis() error {
	repo.Lock()
	defer repo.Unlock()

	return repo.initializeWithGenesis()
}

// Initialize with the genesis header.
func (repo *Repository) initializeWithGenesis() error {
	repo.longest, _ = NewBranch(nil, -1, genesisHeader(repo.config.Network))
	repo.branches = Branches{repo.longest}
	return nil
}

func genesisHeader(net bitcoin.Network) *wire.BlockHeader {
	if net == bitcoin.MainNet {
		// Hash "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
		const mainMerkleRoot = "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b"
		merklehash, _ := bitcoin.NewHash32FromStr(mainMerkleRoot)
		return &wire.BlockHeader{
			Version:    1,
			MerkleRoot: *merklehash,
			Timestamp:  1231006505,
			Bits:       0x1d00ffff,
			Nonce:      2083236893,
		}
	} else { // testnet
		// Hash "000000000933ea01ad0ee984209779baaec3ced90fa3f408719526f8d77f4943"
		const testMerkleRoot = "4a5e1e4baab89f3a32518a88c31bc87f618f76673e2cc77ab2127b7afdeda33b"
		merklehash, _ := bitcoin.NewHash32FromStr(testMerkleRoot)
		return &wire.BlockHeader{
			Version:    1,
			MerkleRoot: *merklehash,
			Timestamp:  1296688602,
			Bits:       0x1d00ffff,
			Nonce:      414098458,
		}
	}
}
