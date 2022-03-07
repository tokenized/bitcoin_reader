package bitcoin_reader

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/logger"
	"github.com/tokenized/pkg/merkle_proof"
	"github.com/tokenized/pkg/threads"
	"github.com/tokenized/pkg/wire"

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

	cancelLock sync.Mutex
	cancelled  bool
	isStarted  bool
	canceller  BlockRequestCanceller

	onComplete OnComplete

	err error

	sync.Mutex
}

func NewBlockDownloader(txProcessor TxProcessor, blockTxManager BlockTxManager, hash bitcoin.Hash32,
	height int, onComplete OnComplete) *BlockDownloader {

	return &BlockDownloader{
		hash:           hash,
		height:         height,
		onComplete:     onComplete,
		txProcessor:    txProcessor,
		blockTxManager: blockTxManager,
		Started:        make(chan interface{}, 1),
		Complete:       make(chan error, 1),
	}
}

func (bd *BlockDownloader) SetCanceller(id uuid.UUID, canceller BlockRequestCanceller) {
	bd.cancelLock.Lock()
	bd.requesterID = id
	bd.canceller = canceller
	bd.cancelLock.Unlock()
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
	hash := bd.Hash()
	height := bd.Height()
	ctx = logger.ContextWithLogFields(ctx,
		logger.Stringer("connection", bd.RequesterID()),
		logger.Stringer("block_hash", hash),
		logger.Int("block_height", height))

	// Wait for download to start
	select {
	case <-interrupt:
		bd.CancelAndWaitForComplete(ctx)
		return threads.Interrupted

	case <-bd.Started:
		bd.cancelLock.Lock()
		bd.isStarted = true
		bd.cancelLock.Unlock()

	case <-time.After(time.Minute):
		logger.Warn(ctx, "Block request timed out")
		bd.CancelAndWaitForComplete(ctx)
		return ErrTimeout

	case err := <-bd.Complete:
		if err != nil && errors.Cause(err) != errBlockDownloadCancelled {
			logger.Warn(ctx, "Block download failed : %s", err)
		}

		if bd.onComplete != nil {
			bd.onComplete(ctx, bd, err)
		}
		if IsCloseError(err) || errors.Cause(err) == errBlockDownloadCancelled {
			return nil
		}
		return err
	}

	// Wait for completion
	select {
	case <-interrupt:
		bd.CancelAndWaitForComplete(ctx)
		return threads.Interrupted

	case <-time.After(time.Hour):
		logger.Warn(ctx, "Block download timed out")
		bd.CancelAndWaitForComplete(ctx)
		return ErrTimeout

	case err := <-bd.Complete:
		if err != nil && !IsCloseError(err) && errors.Cause(err) != errBlockDownloadCancelled {
			logger.Warn(ctx, "Block download failed : %s", err)
		}

		if bd.onComplete != nil {
			bd.onComplete(ctx, bd, err)
		}

		if IsCloseError(err) || errors.Cause(err) == errBlockDownloadCancelled {
			return nil
		}
		return err
	}
}

func (bd *BlockDownloader) CancelAndWaitForComplete(ctx context.Context) {
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

				if bd.onComplete != nil {
					bd.onComplete(ctx, bd, errBlockDownloadCancelled)
				}
				return
			}

			logger.WarnWithFields(ctx, []logger.Field{
				logger.MillisecondsFromNano("elapsed_ms", time.Since(start).Nanoseconds()),
			}, "Waiting for block download cancel")

		case err := <-bd.Complete:
			if err != nil && errors.Cause(err) != errBlockDownloadCancelled {
				logger.Warn(ctx, "Block download failed : %s", err)
			}

			if bd.onComplete != nil {
				bd.onComplete(ctx, bd, err)
			}
			return
		}
	}
}

func (bd *BlockDownloader) Stop(ctx context.Context) {
	hash := bd.Hash()

	isStarted := true
	wasStopped := false
	bd.cancelLock.Lock()
	if !bd.cancelled {
		isStarted = bd.isStarted
		wasStopped = true
	}
	bd.cancelled = true
	bd.cancelLock.Unlock()

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

	signalComplete := false // default to not signalling complete
	signalStarted := false  // default to not signalling started
	bd.cancelLock.Lock()
	if !bd.cancelled {
		if bd.canceller != nil {
			alreadyStarted := bd.canceller.CancelBlockRequest(ctx, hash) // Cancel at connection
			if !alreadyStarted {
				signalComplete = true
			}
		}
		if !bd.isStarted {
			signalStarted = true
		}
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Bool("signal_complete", signalComplete),
			logger.Bool("signal_started", signalStarted),
			logger.Bool("has_canceller", bd.canceller != nil),
		}, "Cancelled block download with canceller")
	}
	bd.cancelled = true
	bd.cancelLock.Unlock()

	if signalStarted {
		// Trigger the "Started" select in "Run".
		waitWarning := logger.NewWaitingWarning(ctx, time.Second, "Signaling started")
		bd.Started <- true
		waitWarning.Cancel()
	}
	if signalComplete {
		// The handler function doesn't need to be waited for so just trigger the "Complete" select
		// in "Run".
		waitWarning := logger.NewWaitingWarning(ctx, time.Second, "Signaling complete")
		bd.Complete <- errBlockDownloadCancelled
		waitWarning.Cancel()
	}
}

func (bd *BlockDownloader) isCancelled() bool {
	bd.cancelLock.Lock()
	result := bd.cancelled
	bd.cancelLock.Unlock()
	return result
}

func (bd *BlockDownloader) HandleBlock(ctx context.Context, header *wire.BlockHeader,
	txCount uint64, txChannel <-chan *wire.MsgTx) error {

	ctx = logger.ContextWithLogFields(ctx,
		logger.Stringer("connection", bd.RequesterID()),
		logger.Stringer("block_hash", bd.Hash()),
		logger.Int("block_height", bd.Height()))

	if bd.isCancelled() {
		logger.Info(ctx, "Block download handler called for cancelled block")
		bd.Complete <- errBlockDownloadCancelled
		return errBlockDownloadCancelled
	}

	requestedHash := bd.Hash()
	hash := *header.BlockHash()

	// Verify this is the correct block
	if !requestedHash.Equal(&hash) {
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Stringer("received_block_hash", hash),
		}, "Wrong block")
		bd.Complete <- errors.Wrap(ErrWrongBlock, hash.String())
		return nil
	}

	bd.Started <- hash

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

		if bd.isCancelled() {
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

	if bd.isCancelled() {
		return errBlockDownloadCancelled
	}

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
