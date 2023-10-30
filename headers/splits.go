package headers

import (
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/wire"
)

var (
	SplitNameBSV = "BSV"
	SplitNameBTC = "BTC"
	SplitNameBCH = "BCH"

	// Chain splits that need to be checked.
	mainNetSplits = []splitHex{
		splitHex{
			name:   SplitNameBTC,
			before: "0000000000000000011865af4122fe3b144e2cbeea86142e8ff2fb4107352d43",
			after:  "00000000000000000019f112ec0a9982926f1258cdcc558dd7c3b7e5dc7fa148",
			height: 478559,
		},
		splitHex{
			name:   SplitNameBCH,
			before: "00000000000000000102d94fde9bd0807a2cc7582fe85dd6349b73ce4e8d9322",
			after:  "0000000000000000004626ff6e3b936941d341c5932ece4357eeccac44e6d56c",
			height: 556767,
		},
	}

	mainNetRequiredSplit = splitHex{
		name:   SplitNameBSV,
		before: "00000000000000000102d94fde9bd0807a2cc7582fe85dd6349b73ce4e8d9322",
		after:  "000000000000000001d956714215d96ffc00e0afda4cd0a96c96f8d802b1662b",
		height: 556767,
	}

	// First block after the BCH/BSV split on the BSV chain.
	mainNetRequiredHeaderPrevBlock, _  = bitcoin.NewHash32FromStr("00000000000000000102d94fde9bd0807a2cc7582fe85dd6349b73ce4e8d9322")
	mainNetRequiredHeaderMerkleRoot, _ = bitcoin.NewHash32FromStr("da2b9eb7e8a3619734a17b55c47bdd6fd855b0afa9c7e14e3a164a279e51bba9")
	MainNetRequiredHeader              = &wire.BlockHeader{
		Version:    536870912,
		PrevBlock:  *mainNetRequiredHeaderPrevBlock,
		MerkleRoot: *mainNetRequiredHeaderMerkleRoot,
		Timestamp:  1542305817,
		Bits:       0x18021fdb,
		Nonce:      1301274612,
	}
)

type Split struct {
	Name       string
	BeforeHash bitcoin.Hash32 // Header on both chains
	AfterHash  bitcoin.Hash32 // Header only on that split
	Height     int            // Height of AfterHash
}

type Splits []Split

// Len is part of sort.Interface.
func (l Splits) Len() int {
	return len(l)
}

// Swap is part of sort.Interface.
func (l Splits) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

// Less is part of sort.Interface. Sorts by highest height first.
func (l Splits) Less(i, j int) bool {
	return l[i].Height > l[j].Height
}

type splitHex struct {
	name   string
	before string // Header on both chains
	after  string // Header only on that split
	height int
}

type HeightHash struct {
	Height int
	Hash   bitcoin.Hash32
}

type HeightHashes []*HeightHash

// Len is part of sort.Interface.
func (l HeightHashes) Len() int {
	return len(l)
}

// Swap is part of sort.Interface.
func (l HeightHashes) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

// Less is part of sort.Interface. Sorts by highest height first.
func (l HeightHashes) Less(i, j int) bool {
	return l[i].Height > l[j].Height
}
