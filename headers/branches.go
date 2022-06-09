package headers

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math/big"
	"time"

	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/logger"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/pkg/wire"

	"github.com/pkg/errors"
)

const (
	branchPath    = "headers/branches"
	branchVersion = uint8(0) // version of branch serialization
)

var (
	ErrHeaderDataNotFound = errors.New("Header Data Not Found")
	ErrWrongPreviousHash  = errors.New("Wrong Previous Hash")
	ErrNotAncestor        = errors.New("Branch Not Ancestor")
)

type Branch struct {
	parent       *Branch           // branch that contains header previous to this branch
	parentHeight int               // height of previous header to first header in this branch
	firstHeader  *wire.BlockHeader // never pruned so we retain the link to the parent
	offset       int               // offset from height to the first header in "headers". used to prune old headers
	headers      []*HeaderData
	heightsMap   map[bitcoin.Hash32]int // map of height by hash
}

type Branches []*Branch

// Len is part of sort.Interface.
func (l Branches) Len() int {
	return len(l)
}

// Swap is part of sort.Interface.
func (l Branches) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

// Less is part of sort.Interface.
func (l Branches) Less(i, j int) bool {
	return l[i].parentHeight < l[j].parentHeight
}

func (b Branch) String(pad string) string {
	result := fmt.Sprintf("\n%sHas parent    : %t\n", pad, b.parent != nil)
	result += fmt.Sprintf("%sParent height : %d\n", pad, b.parentHeight)
	result += fmt.Sprintf("%sFirst hash    : %s\n", pad, b.firstHeader.BlockHash())
	result += fmt.Sprintf("%sOffset        : %d\n", pad, b.offset)
	result += fmt.Sprintf("%sHeader Count  : %d\n", pad, len(b.headers))
	return result
}

func (b Branch) StringHeaderHashes(pad string) string {
	result := "\n"
	for i := b.PrunedLowestHeight(); i <= b.Height(); i++ {
		result += fmt.Sprintf("%sHeader %06d : %s\n", pad, i, b.AtHeight(i).Hash)
	}
	return result
}

// NewBranch
// parent is branch that has the header previous to this header.
// parentHeight is the height in the parent branch of the header previous to this header.
// header is the first header of the new branch.
func NewBranch(parent *Branch, parentHeight int, header *wire.BlockHeader) (*Branch, error) {
	work := &big.Int{}
	hash := *header.BlockHash()

	if parent != nil {
		last := parent.AtHeight(parentHeight)
		if last == nil {
			return nil, ErrHeaderDataNotFound
		}

		if !last.Hash.Equal(&header.PrevBlock) {
			return nil, ErrWrongPreviousHash
		}

		work.Set(last.AccumulatedWork)
	}

	work.Add(work, bitcoin.ConvertToWork(bitcoin.ConvertToDifficulty(header.Bits)))

	// default offset to 1 since first header in this branch is 1 above the height of the previous
	// in the parent branch
	result := &Branch{
		parent:       parent,
		firstHeader:  header,
		parentHeight: parentHeight,
		offset:       1,
		headers: []*HeaderData{&HeaderData{
			Hash:            hash,
			Header:          header,
			AccumulatedWork: work,
		}},
		heightsMap: make(map[bitcoin.Hash32]int),
	}
	result.heightsMap[hash] = result.parentHeight + result.offset
	return result, nil
}

func (b *Branch) Add(header *wire.BlockHeader) bool {
	last := b.Last()
	if !last.Hash.Equal(&header.PrevBlock) {
		return false
	}

	hash := *header.BlockHash()
	work := &big.Int{}
	work.Add(last.AccumulatedWork, bitcoin.ConvertToWork(bitcoin.ConvertToDifficulty(header.Bits)))
	b.headers = append(b.headers, &HeaderData{
		Hash:            hash,
		Header:          header,
		AccumulatedWork: work,
	})
	b.heightsMap[hash] = b.Height()
	return true
}

func (b Branch) Name() string {
	return b.firstHeader.BlockHash().String()
}

func (b Branch) PreviousHash() bitcoin.Hash32 {
	return b.firstHeader.PrevBlock
}

func (b Branch) Last() *HeaderData {
	return b.headers[len(b.headers)-1]
}

func (b Branch) Height() int {
	return b.parentHeight + b.offset + len(b.headers) - 1
}

func (b Branch) Length() int {
	return len(b.headers)
}

// IsLonger returns true if b has more accumulated work than r.
func (b Branch) IsLonger(right *Branch) bool {
	return b.Last().AccumulatedWork.Cmp(right.Last().AccumulatedWork) > 0
}

// AtHeight recursively calls parent branches to get the header for the specified height.
func (b Branch) AtHeight(height int) *HeaderData {
	if height > b.parentHeight {
		offset := height - b.parentHeight - b.offset
		if offset >= len(b.headers) {
			return nil // above tip
		}
		if offset < 0 {
			return nil // pruned and not available
		}

		return b.headers[offset]
	}

	if b.parent == nil {
		return nil
	}

	return b.parent.AtHeight(height)
}

// GetLocatorHashes is used to populate initial P2P header requests. They are designed to quickly
// identify which chain the other node is on.
// This inserts split hashes at the appropriate heights so that if a node is on one of those
// branches then they will respond with the header immediately after the split, which makes it
// easier to identify when a node is on a different chain. It adds them after "max" hashes if they
// didn't fit in anywhere else.
func (b Branch) GetLocatorHashes(splits Splits, delta, max int) HeightHashes {
	height := b.Height()
	if height == 0 {
		return HeightHashes{&HeightHash{0, b.Last().Hash}} // genesis hash
	}

	// Add block hashes in reverse order
	height-- // start with header before last so we will get the last header first in the response
	previousHeight := -1
	var result HeightHashes
	splitAdded := make([]bool, len(splits))
	for {
		if previousHeight != -1 {
			for i, split := range splits {
				if !splitAdded[i] && height < split.Height && previousHeight >= split.Height {
					result = append(result, &HeightHash{
						Height: split.Height,
						Hash:   split.BeforeHash,
					})
					splitAdded[i] = true
				}
			}
		}

		data := b.AtHeight(height)
		if data == nil {
			break // no data at or before this point, pruned
		}

		result = append(result, &HeightHash{
			Height: height,
			Hash:   data.Hash,
		})

		if len(result) >= max {
			break
		}
		if height <= delta {
			break
		}

		previousHeight = height
		height -= delta
		delta *= 2
	}

	for i, split := range splits {
		if !splitAdded[i] && height > split.Height {
			result = append(result, &HeightHash{
				Height: split.Height,
				Hash:   split.BeforeHash,
			})
			splitAdded[i] = true
		}
	}

	return result
}

// Find returns the height of the hash in the branch.
func (b Branch) Find(hash bitcoin.Hash32) int {
	height, exists := b.heightsMap[hash]
	if exists {
		return height
	}

	if b.parent != nil {
		return b.parent.Find(hash)
	}

	return -1
}

// Find returns the branch containing the header and the height in that branch.
func (bs Branches) Find(hash bitcoin.Hash32) (*Branch, int) {
	for _, b := range bs {
		height := b.Find(hash)
		if height != -1 {
			return b, height
		}
	}

	return nil, -1
}

func (bs Branches) Longest() *Branch {
	var result *Branch
	var resultLast *HeaderData
	for _, b := range bs {
		if result == nil {
			result = b
			resultLast = b.Last()
			continue
		}

		last := b.Last()
		if last.AccumulatedWork.Cmp(resultLast.AccumulatedWork) > 0 {
			result = b
			resultLast = last
		}
	}

	return result
}

func (b Branch) CopyEmpty() *Branch {
	return &Branch{
		parent:       b.parent,
		firstHeader:  b.firstHeader,
		parentHeight: b.parentHeight,
		offset:       b.offset,
		heightsMap:   make(map[bitcoin.Hash32]int),
	}
}

func (b *Branch) IntersectHash(other *Branch) *bitcoin.Hash32 {
	current := b
	for {
		if current.parent == nil {
			break
		}

		if current.parent == other {
			return &current.firstHeader.PrevBlock
		}

		current = current.parent
	}

	current = other
	for {
		if current.parent == nil {
			break
		}

		if current.parent == b {
			return &current.firstHeader.PrevBlock
		}

		current = current.parent
	}

	return nil
}

// Consolidate creates a new branch that contains all the headers from the current branch back to
// and including the other branch.
func (b *Branch) Consolidate(ctx context.Context, store storage.Storage,
	other *Branch) (*Branch, int, error) {

	linkBranches := Branches{}
	var linkHeights []int
	var linkHeight int
	current := b
	var previous *Branch
	for {
		if current == other {
			linkBranches = append(linkBranches, current)
			if previous != nil {
				linkHeights = append(linkHeights, previous.parentHeight)
			} else {
				linkHeights = append(linkHeights, -1)
			}
			break
		}

		if current.parent == nil {
			return nil, 0, ErrNotAncestor
		}

		linkHeight = current.parentHeight
		linkBranches = append(linkBranches, current)
		if previous != nil {
			linkHeights = append(linkHeights, previous.parentHeight)
		} else {
			linkHeights = append(linkHeights, -1)
		}
		previous = current
		current = current.parent
	}

	result := other.CopyEmpty()
	height := result.parentHeight + result.offset
	for i := len(linkBranches) - 1; i >= 0; i-- {
		linkBranch := linkBranches[i]
		linkHeight := linkHeights[i]

		if len(result.headers) > 0 {
			lastHash := result.headers[len(result.headers)-1].Hash
			if !linkBranch.firstHeader.PrevBlock.Equal(&lastHash) {
				return nil, 0, fmt.Errorf("Wrong previous hash : got %s, want %s", lastHash,
					linkBranch.firstHeader.PrevBlock)
			}
		}

		if linkBranch.parent != nil { // don't load history for "main" branch
			if err := linkBranch.Reload(ctx, store); err != nil {
				return nil, 0, errors.Wrap(err, "reload")
			}
		}

		// Add remaining headers to the next height
		endHeight := linkHeight - linkBranch.PrunedLowestHeight() + 1
		if linkHeight == -1 {
			endHeight = len(linkBranch.headers)
		}
		for _, header := range linkBranch.headers[:endHeight] {
			result.add(header, height)
			height++
		}
	}

	return result, linkHeight, nil
}

func (b *Branch) add(header *HeaderData, height int) {
	b.headers = append(b.headers, header)
	b.heightsMap[header.Hash] = height
}

// Truncate creates a new branch by removing headers from the beginning of this branch up to the
// point where it matches the other branch and then makes the other branch the parent.
// "parentHeight" must be the height at which both branches have the same header.
func (b *Branch) Truncate(ctx context.Context, store storage.Storage,
	parent *Branch, parentHeight int) (*Branch, error) {

	if parentHeight < b.parentHeight {
		return nil, fmt.Errorf(
			"Truncate cannot extend : current parent height %d, requested parent height %d",
			b.parentHeight, parentHeight)
	}

	if parentHeight+1 < b.PrunedLowestHeight() { // reload if branch height is below available
		if err := b.Reload(ctx, store); err != nil {
			return nil, errors.Wrap(err, "reload")
		}
	}

	// Verify height is correct branch height
	parentHeader := parent.AtHeight(parentHeight)
	if parentHeader == nil {
		return nil, fmt.Errorf("Missing parent header at height %d", parentHeight)
	}

	branchHeader := b.AtHeight(parentHeight + 1)
	if branchHeader == nil {
		return nil, fmt.Errorf("Missing branch header at height %d", parentHeight+1)
	}

	if !branchHeader.Header.PrevBlock.Equal(&parentHeader.Hash) {
		return nil, fmt.Errorf("Wrong hash at new parent height : parent %s, branch %s",
			parentHeader.Hash, branchHeader.Header.PrevBlock)
	}

	result, err := NewBranch(parent, parentHeight, branchHeader.Header)
	if err != nil {
		return nil, errors.Wrap(err, "new branch")
	}

	// Add headers after branch
	height := parentHeight + 2
	startOffset := height - b.PrunedLowestHeight()
	for _, header := range b.headers[startOffset:] {
		result.add(header, height)
		height++
	}

	return result, nil
}

// Connect creates a new branch that connects to the lowest point possible on one of the branches
// specified. "branches" should be sorted by oldest first.
func (b *Branch) Connect(ctx context.Context, store storage.Storage,
	branches Branches) (*Branch, error) {

	if err := b.Reload(ctx, store); err != nil {
		return nil, errors.Wrap(err, "reload")
	}

	var parent *Branch
	var parentHeight int
	for _, branch := range branches {
		parentHeight = branch.Find(b.firstHeader.PrevBlock)
		if parentHeight != -1 {
			parent = branch
			break
		}
	}

	if parent == nil {
		return nil, ErrNotAncestor
	}

	result, err := NewBranch(parent, parentHeight, b.firstHeader)
	if err != nil {
		return nil, errors.Wrap(err, "new branch")
	}

	// Add headers after branch
	height := parentHeight + 1
	startOffset := height - b.PrunedLowestHeight() + 1
	for _, header := range b.headers[startOffset:] {
		result.add(header, height)
		height++
	}

	return result, nil
}

// Link links a branch to its parent after loading from storage. "branches" should be sorted by
// oldest first.
func (b *Branch) Link(branches Branches) error {
	for _, branch := range branches {
		height := branch.Find(b.firstHeader.PrevBlock)
		if height == -1 {
			continue
		}

		if height != b.parentHeight {
			return fmt.Errorf("Wrong parent height : height %d, parent height %d", height,
				b.parentHeight)
		}

		b.parent = branch
		return nil
	}

	return ErrNotAncestor
}

// Trim removes the header at the specified height and everything above.
func (b *Branch) Trim(height int) error {
	if height <= b.parentHeight {
		return errors.New("Height Below Start") // not in this branch
	}

	offset := height - b.parentHeight - b.offset
	if offset >= len(b.headers) {
		return errors.New("Height Above Tip") // above tip
	}

	b.headers = b.headers[:offset]
	return nil
}

// Trim removes the header at the specified height in the specified branch and everything above.
// It also removes any branches that are descendents of that.
func (bs *Branches) Trim(branch *Branch, height int) error {
	if height == branch.parentHeight+1 {
		// Remove branch
		for i, b := range *bs {
			if b == branch {
				*bs = append((*bs)[:i], (*bs)[i+1:]...)
				break
			}
		}
	} else {
		if err := branch.Trim(height); err != nil {
			return errors.Wrap(err, "trim including branch")
		}
	}

	var removedBranches Branches
	var newBranches Branches
	for _, b := range *bs {
		if removedBranches.Includes(b.parent) {
			removedBranches = append(removedBranches, b)
			continue // remove branch
		}

		if b.parent == branch && b.parentHeight >= height {
			removedBranches = append(removedBranches, b)
			continue // remove branch
		}

		newBranches = append(newBranches, b)
	}

	*bs = newBranches
	return nil
}

func (bs Branches) Includes(branch *Branch) bool {
	for _, b := range bs {
		if b == branch {
			return true
		}
	}

	return false
}

// AvailableHeight is the lowest height data that is available. It does include parent branches.
func (b *Branch) AvailableHeight() int {
	if b.offset > 1 || b.parent == nil {
		// Branch has been pruned or has no parent
		return b.parentHeight + b.offset
	}

	return b.parent.AvailableHeight()
}

func (b *Branch) Prune(count int) {
	if count < 0 || count >= len(b.headers) {
		return // already pruned above that height
	}

	for _, data := range b.headers[:count] {
		delete(b.heightsMap, data.Hash)
	}
	b.headers = b.headers[count:]
	b.offset += count
}

// PrunedLowestHeight is the lowest height data that is available on this branch. It can be above
// the branch start height because of pruning. It does not include parent branches.
func (b *Branch) PrunedLowestHeight() int {
	return b.parentHeight + b.offset
}

func (b Branch) path() string {
	return fmt.Sprintf("%s/%s", branchPath, b.firstHeader.BlockHash())
}

func LoadBranch(ctx context.Context, store storage.Storage, hash bitcoin.Hash32) (*Branch, error) {
	path := fmt.Sprintf("%s/%s", branchPath, hash)
	data, err := store.Read(ctx, path)
	if err != nil {
		return nil, errors.Wrapf(err, "read : %s", path)
	}

	result := &Branch{}
	if err := result.Deserialize(bytes.NewReader(data)); err != nil {
		return nil, errors.Wrap(err, "deserialize")
	}

	// Load height map
	result.heightsMap = make(map[bitcoin.Hash32]int)
	height := result.parentHeight + result.offset
	for _, header := range result.headers {
		result.heightsMap[header.Hash] = height
		height++
	}

	return result, nil
}

// Reload loads all headers from storage if they have been pruned.
func (b *Branch) Reload(ctx context.Context, store storage.Storage) error {
	if b.offset == 1 {
		return nil // already full
	}

	path := b.path()
	data, err := store.Read(ctx, path)
	if err != nil {
		return errors.Wrapf(err, "read : %s", path)
	}

	previousBranch := &Branch{}
	if err := previousBranch.Deserialize(bytes.NewReader(data)); err != nil {
		return errors.Wrap(err, "deserialize")
	}

	appendOffset := b.offset - previousBranch.offset
	b.headers = append(previousBranch.headers[:appendOffset], b.headers...)
	b.offset = previousBranch.offset

	// Append to height map
	height := b.parentHeight + b.offset
	for _, header := range b.headers[:appendOffset] {
		b.heightsMap[header.Hash] = height
		height++
	}

	return nil
}

func (b Branch) Save(ctx context.Context, store storage.Storage) error {
	start := time.Now()
	path := b.path()

	// Load previous data
	data, err := store.Read(ctx, path)
	if err != nil {
		if errors.Cause(err) != storage.ErrNotFound {
			return errors.Wrapf(err, "read : %s", path)
		}

		// No previous data so just write current data.
		buf := &bytes.Buffer{}
		if err := b.Serialize(buf); err != nil {
			return errors.Wrap(err, "serialize")
		}

		if err := store.Write(ctx, path, buf.Bytes(), nil); err != nil {
			return errors.Wrapf(err, "write : %s", path)
		}

		return nil
	}

	previousBranch := &Branch{}
	if err := previousBranch.Deserialize(bytes.NewReader(data)); err != nil {
		return errors.Wrap(err, "deserialize")
	}

	// Append new headers
	appendOffset := b.offset - previousBranch.offset
	previousBranch.headers = append(previousBranch.headers[:appendOffset], b.headers...)

	buf := &bytes.Buffer{}
	if err := previousBranch.Serialize(buf); err != nil {
		return errors.Wrap(err, "serialize")
	}

	if err := store.Write(ctx, path, buf.Bytes(), nil); err != nil {
		return errors.Wrapf(err, "write : %s", path)
	}

	logger.ElapsedWithFields(ctx, start, []logger.Field{
		logger.String("branch", b.Name()),
		logger.Int("length", b.Length()),
		logger.Int("block_height", b.Height()),
	}, "Saved branch")

	return nil
}

func (b Branch) Serialize(w io.Writer) error {
	if err := binary.Write(w, endian, branchVersion); err != nil {
		return errors.Wrap(err, "version")
	}

	if err := b.firstHeader.Serialize(w); err != nil {
		return errors.Wrap(err, "first header")
	}

	if err := binary.Write(w, endian, int64(b.parentHeight)); err != nil {
		return errors.Wrap(err, "parent height")
	}

	if err := binary.Write(w, endian, int64(b.offset)); err != nil {
		return errors.Wrap(err, "offset")
	}

	if err := binary.Write(w, endian, uint32(len(b.headers))); err != nil {
		return errors.Wrap(err, "count")
	}

	for i, header := range b.headers {
		if err := header.Serialize(w); err != nil {
			return errors.Wrapf(err, "header %d", i)
		}
	}

	return nil
}

func (b *Branch) Deserialize(r io.Reader) error {
	var version uint8
	if err := binary.Read(r, endian, &version); err != nil {
		return errors.Wrap(err, "version")
	}

	if version != 0 {
		return fmt.Errorf("Unsupported branch serialize version : %d", version)
	}

	b.firstHeader = &wire.BlockHeader{}
	if err := b.firstHeader.Deserialize(r); err != nil {
		return errors.Wrap(err, "first header")
	}

	var parentHeight int64
	if err := binary.Read(r, endian, &parentHeight); err != nil {
		return errors.Wrap(err, "parent height")
	}
	b.parentHeight = int(parentHeight)

	var offset int64
	if err := binary.Read(r, endian, &offset); err != nil {
		return errors.Wrap(err, "offset")
	}
	b.offset = int(offset)

	var count uint32
	if err := binary.Read(r, endian, &count); err != nil {
		return errors.Wrap(err, "count")
	}

	b.headers = make([]*HeaderData, count)
	for i := range b.headers {
		header := &HeaderData{}
		if err := header.Deserialize(r); err != nil {
			return errors.Wrapf(err, "header %d", i)
		}

		b.headers[i] = header
	}

	return nil
}
