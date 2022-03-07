package bitcoin_reader

import (
	"context"

	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/merkle_proof"
	"github.com/tokenized/pkg/wire"

	"github.com/google/uuid"
)

type TxProcessor interface {
	// ProcessTx returns true if the tx is relevant.
	ProcessTx(ctx context.Context, tx *wire.MsgTx) (bool, error)

	// CancelTx specifies that a tx is no longer valid because a conflicting tx has been confirmed.
	CancelTx(ctx context.Context, txid bitcoin.Hash32) error

	// AddTxConflict specifies that there is an unconfirmed conflicting tx to a relevant tx.
	AddTxConflict(ctx context.Context, txid, conflictTxID bitcoin.Hash32) error

	ConfirmTx(ctx context.Context, txid bitcoin.Hash32, blockHeight int,
		merkleProof *merkle_proof.MerkleProof) error

	UpdateTxChainDepth(ctx context.Context, txid bitcoin.Hash32, chainDepth int) error

	ProcessCoinbaseTx(ctx context.Context, blockHash bitcoin.Hash32, tx *wire.MsgTx) error
}

type TxSaver interface {
	SaveTx(context.Context, *wire.MsgTx) error
}

// HandleBlock handles a block coming from a data source.
type HandleBlock func(ctx context.Context, header *wire.BlockHeader, txCount uint64,
	txChannel <-chan *wire.MsgTx) error

type OnStop func(context.Context)

type BlockRequestor interface {
	// RequestBlock requests a block from a data source.
	// "handler" handles the block data as it is provided.
	// "onStop" is a function that is called if the data source stops. It should abort the request
	// from the handler side.
	RequestBlock(ctx context.Context, hash bitcoin.Hash32,
		handler HandleBlock, onStop OnStop) (BlockRequestCanceller, error)
}

type BlockRequestCanceller interface {
	// ID returns the unique id of block requestor.
	ID() uuid.UUID

	// CancelBlockRequest cancels a request for a block. It returns true if the block handler has
	// already been called.
	CancelBlockRequest(context.Context, bitcoin.Hash32) bool
}

type BlockTxManager interface {
	// FetchBlockTxIDs fetches the relevant txids for a block hash. Returns false if data doesn't
	// exist for the block hash, which means the block has not been processed yet.
	FetchBlockTxIDs(ctx context.Context, blockHash bitcoin.Hash32) ([]bitcoin.Hash32, bool, error)

	AppendBlockTxIDs(ctx context.Context, blockHash bitcoin.Hash32, txids []bitcoin.Hash32) error
}
