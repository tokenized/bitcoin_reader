package bitcoin_reader

import (
	"time"

	"github.com/tokenized/pkg/bitcoin"
)

type Config struct {
	Network bitcoin.Network `default:"mainnet" json:"network" envconfig:"NETWORK"`

	// Timeout is the amount of time a node will remain connected
	Timeout time.Duration `default:"4h" json:"timeout" envconfig:"NODE_TIMEOUT"`

	// ScanCount is the number of peer addresses that will be scanned for valid peers when a scan is
	// performed.
	ScanCount int `default:"100" json:"scan_count" envconfig:"SCAN_COUNT"`

	// TxRequestCount is the maximum number of txs that will be requested from a node at one time.
	TxRequestCount int `default:"10000" json:"tx_request_count" envconfig:"TX_REQUEST_COUNT"`

	// StartupDelay delay after node manager startup before block processing will begin.
	StartupDelay time.Duration `default:"1m" json:"startup_delay" envconfig:"STARTUP_DELAY"`

	// ConcurrentBlockRequests is the number of concurrent block requests that will be attempted in
	// case some of the requests are slow.
	ConcurrentBlockRequests int `default:"2" json:"concurrent_block_requests" envconfig:"CONCURRENT_BLOCK_REQUESTS"`

	// DesiredNodeCount is the number of node connectes that should be maintained.
	DesiredNodeCount int `default:"50" json:"desired_node_count" envconfig:"DESIRED_NODE_COUNT"`

	// StartBlockHeight is the block height at which blocks should be downloaded and processed.
	// Only headers will be collected for blocks below that height.
	StartBlockHeight int `default:"700000" json:"start_block_height" envconfig:"START_BLOCK_HEIGHT"`

	// BlockRequestDelay is the delay between concurrent block requests. It should be long enough
	// that small blocks will complete before the second request is made and only use concurrent
	// requests for slow or large blocks.
	BlockRequestDelay time.Duration `default:"5s" json:"block_request_delay" envconfig:"BLOCK_REQUEST_DELAY"`
}

func DefaultConfig() *Config {
	return &Config{
		Network:                 bitcoin.MainNet,
		Timeout:                 time.Hour * 4,
		ScanCount:               1000,
		TxRequestCount:          10000,
		StartupDelay:            time.Minute,
		ConcurrentBlockRequests: 2,
		DesiredNodeCount:        50,
		StartBlockHeight:        700000,
		BlockRequestDelay:       time.Second * 5,
	}
}
