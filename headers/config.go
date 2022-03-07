package headers

import "github.com/tokenized/pkg/bitcoin"

type Config struct {
	Network bitcoin.Network `default:"mainnet" envconfig:"NETWORK" json:"network"`

	// MaxBranchDepth is the max depth, from the longest height, that a new branch will be created.
	// This helps prevent "hidden mining" where a miner doesn't release there blocks until later.
	MaxBranchDepth int `default:"144" envconfig:"MAX_BRANCH_DEPTH" json:"max_branch_depth"`

	// InvalidHeaderHashes are header hashes that will not be accepted as valid. These can also be
	// added/removed via a function call and will be retained in storage. After added via config
	// and the repo is saved then these will be retained even if removed from the config. To remove
	// them after that a function call must be performed. The BTC and BCH split headers are handled
	// by a separate "split" system for handling other chains.
	InvalidHeaderHashes []bitcoin.Hash32 `envconfig:"INVALID_HEADER_HASHES" json:"invalid_header_hashes"`
}

func DefaultConfig() *Config {
	return &Config{
		Network:        bitcoin.MainNet,
		MaxBranchDepth: 144,
	}
}
