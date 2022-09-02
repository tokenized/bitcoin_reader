package bitcoin_reader

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/merkle_proof"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/threads"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

var (
	errBlockDownloadCancelled = errors.New("Block Download Cancelled")
)

type OnComplete func(context.Context, *BlockDownloader, error)

type BlockDownloader struct {
	id          uuid.UUID
	requesterID uuid.UUID
	hash        bitcoin.Hash32
	height      int

	txProcessor    TxProcessor
	blockTxManager BlockTxManager

	Started  chan interface{}
	Complete chan error

	stateLock   sync.Mutex
	isCancelled bool
	isStarted   bool
	isComplete  bool
	canceller   BlockRequestCanceller

	err error

	sync.Mutex
}

func NewBlockDownloader(txProcessor TxProcessor, blockTxManager BlockTxManager, hash bitcoin.Hash32,
	height int) *BlockDownloader {

	return &BlockDownloader{
		hash:           hash,
		height:         height,
		txProcessor:    txProcessor,
		blockTxManager: blockTxManager,
		Started:        make(chan interface{}, 2),
		Complete:       make(chan error, 2),
	}
}

func (bd *BlockDownloader) SetCanceller(id uuid.UUID, canceller BlockRequestCanceller) {
	bd.stateLock.Lock()
	bd.requesterID = id
	bd.canceller = canceller
	bd.stateLock.Unlock()
}

func (bd *BlockDownloader) ID() uuid.UUID {
	bd.Lock()
	result := bd.id
	bd.Unlock()

	return result
}

func (bd *BlockDownloader) RequesterID() uuid.UUID {
	bd.Lock()
	result := bd.requesterID
	bd.Unlock()

	return result
}

func (bd *BlockDownloader) Hash() bitcoin.Hash32 {
	bd.Lock()
	result := bd.hash
	bd.Unlock()

	return result
}

func (bd *BlockDownloader) Height() int {
	bd.Lock()
	result := bd.height
	bd.Unlock()

	return result
}

func (bd *BlockDownloader) Error() error {
	bd.Lock()
	result := bd.err
	bd.Unlock()

	return result
}

func (bd *BlockDownloader) Run(ctx context.Context, interrupt <-chan interface{}) error {
	start := time.Now()
	hash := bd.Hash()
	height := bd.Height()
	ctx = logger.ContextWithLogFields(ctx,
		logger.Stringer("connection", bd.RequesterID()),
		logger.Stringer("block_hash", hash),
		logger.Int("block_height", height))

	// Wait for download to start
	select {
	case <-interrupt:
		bd.cancelAndWaitForComplete(ctx)
		return threads.Interrupted

	case <-bd.Started:
		bd.stateLock.Lock()
		bd.isStarted = true
		bd.stateLock.Unlock()

	// We must start receiving the block before this time, otherwise it is a slow node or the node
	// is ignoring our request.
	case <-time.After(2 * time.Minute):
		logger.WarnWithFields(ctx, []logger.Field{
			logger.MillisecondsFromNano("elapsed_ms", time.Since(start).Nanoseconds()),
		}, "Block request timed out")
		bd.cancelAndWaitForComplete(ctx)
		return ErrTimeout

	case err := <-bd.Complete:
		bd.stateLock.Lock()
		bd.isComplete = true
		bd.stateLock.Unlock()
		if err != nil && errors.Cause(err) != errBlockDownloadCancelled {
			logger.Warn(ctx, "Block download failed : %s", err)
		}

		return err
	}

	// Wait for completion
	select {
	case <-interrupt:
		bd.cancelAndWaitForComplete(ctx)
		return threads.Interrupted

	case <-time.After(time.Hour):
		logger.WarnWithFields(ctx, []logger.Field{
			logger.MillisecondsFromNano("elapsed_ms", time.Since(start).Nanoseconds()),
		}, "Block download timed out")
		return ErrTimeout

	case err := <-bd.Complete:
		bd.stateLock.Lock()
		bd.isComplete = true
		bd.stateLock.Unlock()
		if err != nil && !IsCloseError(err) && errors.Cause(err) != errBlockDownloadCancelled {
			logger.Warn(ctx, "Block download failed : %s", err)
		}

		return err
	}
}

func (bd *BlockDownloader) cancelAndWaitForComplete(ctx context.Context) {
	bd.Cancel(ctx)

	count := 0
	start := time.Now()
	for {
		select {
		case <-time.After(time.Second * 10):
			count++

			if count >= 60 {
				logger.WarnWithFields(ctx, []logger.Field{
					logger.MillisecondsFromNano("elapsed_ms", time.Since(start).Nanoseconds()),
				}, "Block cancel timed out")

				return
			}

			logger.WarnWithFields(ctx, []logger.Field{
				logger.MillisecondsFromNano("elapsed_ms", time.Since(start).Nanoseconds()),
			}, "Waiting for block download cancel")

		case err := <-bd.Complete:
			bd.stateLock.Lock()
			bd.isComplete = true
			bd.stateLock.Unlock()
			if err != nil && errors.Cause(err) != errBlockDownloadCancelled {
				logger.Warn(ctx, "Block download failed : %s", err)
			}

			return
		}
	}
}

func (bd *BlockDownloader) Stop(ctx context.Context) {
	hash := bd.Hash()

	isStarted := true
	wasStopped := false
	bd.stateLock.Lock()
	if bd.isComplete {
		bd.stateLock.Unlock()
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Stringer("connection", bd.RequesterID()),
			logger.Stringer("block_hash", hash),
			logger.Int("block_height", bd.Height()),
		}, "Stopping block download that already completed")
		return
	}

	if !bd.isCancelled {
		isStarted = bd.isStarted
		wasStopped = true
	}
	bd.isCancelled = true
	bd.stateLock.Unlock()

	if !isStarted {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("connection", bd.RequesterID()),
			logger.Stringer("block_hash", hash),
			logger.Int("block_height", bd.Height()),
		}, "Stopping block download that hasn't started")
		bd.Started <- true
		bd.Complete <- errBlockDownloadCancelled
	}

	if wasStopped {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("connection", bd.RequesterID()),
			logger.Stringer("block_hash", hash),
			logger.Int("block_height", bd.Height()),
		}, "Stopped block download")
	}
}

func (bd *BlockDownloader) Cancel(ctx context.Context) {
	hash := bd.Hash()
	ctx = logger.ContextWithLogFields(ctx,
		logger.Stringer("connection", bd.RequesterID()),
		logger.Stringer("block_hash", hash),
		logger.Int("block_height", bd.Height()))

	sendComplete := false // default to not sending complete signal
	sendStarted := false  // default to not sending started signal
	bd.stateLock.Lock()
	if bd.isComplete {
		bd.stateLock.Unlock()
		logger.Warn(ctx, "Attempted cancel of block download that was already complete")
		return
	}

	if !bd.isCancelled {
		if bd.canceller != nil {
			alreadyStarted := bd.canceller.CancelBlockRequest(ctx, hash) // Cancel at connection
			if !alreadyStarted {
				sendComplete = true
			}
		}
		if !bd.isStarted {
			sendStarted = true
		}
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Bool("send_complete", sendComplete),
			logger.Bool("send_started", sendStarted),
			logger.Bool("has_canceller", bd.canceller != nil),
		}, "Cancelled block download with canceller")
	}
	bd.isCancelled = true
	bd.stateLock.Unlock()

	if sendStarted {
		// Trigger the "Started" select in "Run".
		bd.Started <- true
	}
	if sendComplete {
		// The handler function doesn't need to be waited for so just trigger the "Complete" select
		// in "Run".
		bd.Complete <- errBlockDownloadCancelled
	}
}

func (bd *BlockDownloader) wasCancelled() bool {
	bd.stateLock.Lock()
	result := bd.isCancelled
	bd.stateLock.Unlock()
	return result
}

func (bd *BlockDownloader) HandleBlock(ctx context.Context, header *wire.BlockHeader,
	txCount uint64, txChannel <-chan *wire.MsgTx) error {

	hash := *header.BlockHash()
	bd.Started <- hash

	ctx = logger.ContextWithLogFields(ctx,
		logger.Stringer("connection", bd.RequesterID()),
		logger.Stringer("block_hash", bd.Hash()),
		logger.Int("block_height", bd.Height()))

	if bd.wasCancelled() {
		logger.Warn(ctx, "Block download handler called for cancelled block")
		bd.Complete <- errBlockDownloadCancelled
		return errBlockDownloadCancelled
	}

	requestedHash := bd.Hash()

	// Verify this is the correct block
	if !requestedHash.Equal(&hash) {
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Stringer("received_block_hash", hash),
		}, "Wrong block")
		bd.Complete <- errors.Wrap(ErrWrongBlock, hash.String())
		return nil
	}

	err := bd.handleBlock(ctx, header, txCount, txChannel)
	if err != nil && errors.Cause(err) != errBlockDownloadCancelled {
		logger.Warn(ctx, "Failed to handle block : %s", err)
	}

	bd.Complete <- err
	return err // return error to node
}

func (bd *BlockDownloader) handleBlock(ctx context.Context, header *wire.BlockHeader,
	txCount uint64, txChannel <-chan *wire.MsgTx) error {
	start := time.Now()

	hash := bd.Hash()
	height := bd.Height()

	// Process block txs
	var blockTxIDs []bitcoin.Hash32

	merkleTree := merkle_proof.NewMerkleTree(true)
	var coinbaseTx *wire.MsgTx
	txByteCount := 0
	i := 0
	for tx := range txChannel {
		txByteCount += tx.SerializeSize()
		txid := *tx.TxHash()

		if i == 0 {
			coinbaseTx = tx
		}

		isRelevant, err := bd.txProcessor.ProcessTx(ctx, tx)
		if err != nil {
			for range txChannel { // flush channel
			}
			return errors.Wrap(err, "process tx")
		}

		if isRelevant {
			blockTxIDs = append(blockTxIDs, txid)
			merkleTree.AddMerkleProof(txid)
		}

		merkleTree.AddHash(txid)

		if bd.wasCancelled() {
			for range txChannel { // flush channel
			}
			return errBlockDownloadCancelled
		}

		i++
	}

	if uint64(i) != txCount {
		return errBlockDownloadCancelled
	}

	// Check merkle root hash
	merkleRootHash, merkleProofs := merkleTree.FinalizeMerkleProofs()
	if !merkleRootHash.Equal(&header.MerkleRoot) {
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Stringer("calculated", merkleRootHash),
			logger.Stringer("header", header.MerkleRoot),
		}, "Invalid merkle root")
		return merkle_proof.ErrWrongMerkleRoot
	}

	// Merkle proofs should be in same order as relevant txs and send updated status to server.
	if len(merkleProofs) != len(blockTxIDs) {
		return fmt.Errorf("Wrong merkle proof count : got %d, want %d", len(merkleProofs),
			len(blockTxIDs))
	}

	if bd.wasCancelled() {
		return errBlockDownloadCancelled
	}

	// Don't process coinbase or confirms until after merkle root is verified, in case block is
	// malicious and the wrong txs are provided.
	if err := bd.txProcessor.ProcessCoinbaseTx(ctx, hash, coinbaseTx); err != nil {
		return errors.Wrap(err, "process coinbase tx")
	}

	for i, txid := range blockTxIDs {
		merkleProofs[i].BlockHeader = header
		merkleProofs[i].BlockHash = &hash

		if err := bd.txProcessor.ConfirmTx(ctx, txid, height, merkleProofs[i]); err != nil {
			return errors.Wrap(err, "confirm tx")
		}
	}

	if err := bd.blockTxManager.AppendBlockTxIDs(ctx, hash, blockTxIDs); err != nil {
		return errors.Wrap(err, "save block txids")
	}

	logger.ElapsedWithFields(ctx, start, []logger.Field{
		logger.Uint64("tx_count", txCount),
		logger.Float64("block_size_mb",
			float64(txByteCount+80+wire.VarIntSerializeSize(txCount))/1e6),
		logger.Int("relevant_tx_count", len(blockTxIDs)),
	}, "Processed block")
	return nil
}
