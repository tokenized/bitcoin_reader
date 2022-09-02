package bitcoin_reader

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/threads"

	"github.com/pkg/errors"
)

var (
	ErrWrongBlock = errors.New("Wrong Block")

	BlockAborted = errors.New("Block Aborted")
)

type BlockManager struct {
	blockTxManager          BlockTxManager
	requestor               BlockRequestor
	concurrentBlockRequests int
	blockRequestDelay       time.Duration

	requests       chan *downloadRequest
	requestsClosed bool
	requestLock    sync.Mutex

	downloaders    []*downloadThread
	downloaderLock sync.Mutex

	currentHash       bitcoin.Hash32
	currentIsComplete bool
	currentComplete   chan interface{}
	currentLock       sync.Mutex

	sync.Mutex
}

type downloadThread struct {
	downloader *BlockDownloader
	thread     *threads.InterruptableThread
}

type downloadRequest struct {
	hash      bitcoin.Hash32
	height    int
	processor TxProcessor
	complete  chan error
	abort     chan interface{}
}

func NewBlockManager(blockTxManager BlockTxManager, requestor BlockRequestor,
	concurrentBlockRequests int, blockRequestDelay time.Duration) *BlockManager {

	return &BlockManager{
		blockTxManager:          blockTxManager,
		requestor:               requestor,
		concurrentBlockRequests: concurrentBlockRequests,
		blockRequestDelay:       blockRequestDelay,
		requests:                make(chan *downloadRequest, 10),
	}
}

func (m *BlockManager) AddRequest(ctx context.Context, hash bitcoin.Hash32, height int,
	processor TxProcessor) (<-chan error, chan<- interface{}) {

	request := &downloadRequest{
		hash:      hash,
		height:    height,
		processor: processor,
		complete:  make(chan error),
		abort:     make(chan interface{}),
	}

	m.requestLock.Lock()
	if m.requestsClosed {
		m.requestLock.Unlock()
		return nil, nil
	}
	m.requests <- request
	m.requestLock.Unlock()

	return request.complete, request.abort
}

func (m *BlockManager) close(blockInterrupt chan<- interface{}) {
	close(blockInterrupt)

	m.requestLock.Lock()
	m.requestsClosed = true
	close(m.requests)
	m.requestLock.Unlock()
}

func (m *BlockManager) Run(ctx context.Context, interrupt <-chan interface{}) error {
	defer func() {
		m.Stop(ctx)
		m.shutdown(ctx)
	}()

	abortInterrupt := make(chan interface{})
	blockInterrupt := make(chan interface{})
	go func() {
		select {
		case <-abortInterrupt:
			m.close(blockInterrupt)
		case <-interrupt:
			m.close(blockInterrupt)
		}
	}()

	for request := range m.requests {
		if err := m.processRequest(ctx, request, blockInterrupt); err != nil {
			close(abortInterrupt)
			for range m.requests { // flush channel
			}

			if errors.Cause(err) == threads.Interrupted {
				return nil
			}

			logger.ErrorWithFields(ctx, []logger.Field{
				logger.Stringer("block_hash", request.hash),
				logger.Int("block_height", request.height),
			}, "Failed to process request : %s", err)

			return errors.Wrap(err, "process request")
		}
	}

	return nil
}

func (m *BlockManager) Stop(ctx context.Context) {
	logger.InfoWithFields(ctx, []logger.Field{
		logger.Int("downloader_count", len(m.downloaders)),
	}, "Stopping block manager")

	m.downloaderLock.Lock()
	downloaders := make([]*downloadThread, len(m.downloaders))
	copy(downloaders, m.downloaders)
	m.downloaderLock.Unlock()

	for _, dt := range downloaders {
		dt.downloader.Cancel(ctx)
		dt.thread.Stop(ctx)
	}
}

func (m *BlockManager) shutdown(ctx context.Context) {
	start := time.Now()
	count := 0
	for {
		m.downloaderLock.Lock()
		activeCount := len(m.downloaders)
		if count >= 30 {
			for _, dt := range m.downloaders {
				logger.WarnWithFields(ctx, []logger.Field{
					logger.Stringer("connection", dt.downloader.RequesterID()),
					logger.Stringer("block_hash", dt.downloader.Hash()),
					logger.Int("block_height", dt.downloader.Height()),
				}, "Waiting for: Block Downloader Shutdown")
			}
		}
		m.downloaderLock.Unlock()

		if activeCount == 0 {
			logger.Info(ctx, "Finished Block Manager")
			return
		}

		if count >= 30 {
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Timestamp("start", start.UnixNano()),
				logger.MillisecondsFromNano("elapsed_ms", time.Since(start).Nanoseconds()),
				logger.Int("active_downloaders", activeCount),
			}, "Waiting for: Block Manager Shutdown")
			count = 0
		}

		count++
		time.Sleep(time.Millisecond * 100)
	}
}

func (m *BlockManager) processRequest(ctx context.Context, request *downloadRequest,
	interrupt <-chan interface{}) error {

	ctx = logger.ContextWithLogFields(ctx, logger.Stringer("block_hash", request.hash),
		logger.Int("block_height", request.height))

	logger.Verbose(ctx, "Starting block request")

	m.currentLock.Lock()
	m.currentHash = request.hash
	m.currentIsComplete = false
	m.currentComplete = make(chan interface{})
	m.currentLock.Unlock()

	// Send initial block request.
	countWithoutActiveDownload := 0
	if err := m.requestBlock(ctx, request.hash, request.height, request.processor); err != nil {
		logger.Warn(ctx, "Failed to request block : %s", err)
	}

	// Wait for a download to complete and send new block requests as necessary.
	for {
		select {
		case <-time.After(m.blockRequestDelay): // most blocks finish within 5 seconds
			downloaders := m.Downloaders(request.hash)
			logger.VerboseWithFields(ctx, []logger.Field{
				logger.Stringers("active_downloads", downloaders),
			}, "Active block downloads")
			activeDownloadCount := len(downloaders)
			if activeDownloadCount > 0 {
				countWithoutActiveDownload = 0
			} else {
				countWithoutActiveDownload++
			}

			if activeDownloadCount < m.concurrentBlockRequests {
				if err := m.requestBlock(ctx, request.hash, request.height,
					request.processor); err != nil {
					logger.Warn(ctx, "Failed to request block : %s", err)
				} else {
					activeDownloadCount++
				}
			}

			if countWithoutActiveDownload > 20 {
				return ErrNodeNotAvailable
			}

		case <-interrupt:
			return threads.Interrupted

		case <-request.abort:
			m.cancelDownloaders(ctx, request.hash)
			request.complete <- BlockAborted
			return nil

		case <-m.currentComplete:
			m.cancelDownloaders(ctx, request.hash) // stop any others still downloading
			close(request.complete)
			return nil
		}
	}
}

func (m *BlockManager) DownloaderCount(hash bitcoin.Hash32) int {
	result := 0
	m.downloaderLock.Lock()
	for _, dt := range m.downloaders {
		dh := dt.downloader.Hash()
		if hash.Equal(&dh) {
			result++
		}
	}
	m.downloaderLock.Unlock()
	return result
}

func (m *BlockManager) Downloaders(hash bitcoin.Hash32) []fmt.Stringer {
	var result []fmt.Stringer
	m.downloaderLock.Lock()
	for _, dt := range m.downloaders {
		dh := dt.downloader.Hash()
		if hash.Equal(&dh) {
			result = append(result, dt.downloader.RequesterID())
		}
	}
	m.downloaderLock.Unlock()
	return result
}

func (m *BlockManager) cancelDownloaders(ctx context.Context, hash bitcoin.Hash32) {
	m.downloaderLock.Lock()
	downloaders := make([]*downloadThread, len(m.downloaders))
	copy(downloaders, m.downloaders)
	m.downloaderLock.Unlock()

	for _, dt := range downloaders {
		dh := dt.downloader.Hash()
		if hash.Equal(&dh) {
			logger.VerboseWithFields(ctx, []logger.Field{
				logger.Stringer("connection", dt.downloader.RequesterID()),
				logger.Stringer("block_hash", dt.downloader.Hash()),
				logger.Int("block_height", dt.downloader.Height()),
			}, "Cancelling block downloader")
			dt.downloader.Cancel(ctx)
			dt.thread.Stop(ctx)
		}
	}
}

func (m *BlockManager) requestBlock(ctx context.Context, hash bitcoin.Hash32, height int,
	processor TxProcessor) error {

	logger.Verbose(ctx, "Creating block request")
	downloader := NewBlockDownloader(processor, m.blockTxManager, hash, height)

	node, err := m.requestor.RequestBlock(ctx, hash, downloader.HandleBlock, downloader.Stop)
	if err != nil {
		return err
	}

	nodeID := node.ID()
	downloader.SetCanceller(nodeID, node)

	dt := &downloadThread{
		downloader: downloader,
		thread: threads.NewInterruptableThread(fmt.Sprintf("Download Block: %s", hash),
			downloader.Run),
	}
	df := &downloadFinisher{
		manager:    m,
		downloader: downloader,
	}

	onCompleteChannel := dt.thread.GetCompleteChannel()
	onCompleteThread := threads.NewUninterruptableThread(fmt.Sprintf("On Block Complete: %s", hash),
		func(ctx context.Context) error {
			err, ok := <-onCompleteChannel
			if ok {
				df.onDownloaderCompleted(ctx, err)
			}
			return nil
		})

	m.downloaderLock.Lock()
	m.downloaders = append(m.downloaders, dt)
	m.downloaderLock.Unlock()

	// Start download thread
	ctx = logger.ContextWithLogFields(ctx, logger.Stringer("connection", nodeID))
	dt.thread.Start(ctx)
	onCompleteThread.Start(ctx)
	return nil
}

func (m *BlockManager) removeDownloader(ctx context.Context, downloader *BlockDownloader) {
	m.downloaderLock.Lock()
	for i, dt := range m.downloaders {
		if dt.downloader == downloader {
			m.downloaders = append(m.downloaders[:i], m.downloaders[i+1:]...)
			m.downloaderLock.Unlock()
			return
		}
	}

	logger.Warn(ctx, "Block downloader not found to remove")
	m.downloaderLock.Unlock()
}

func (m *BlockManager) markBlockRequestComplete(ctx context.Context, hash bitcoin.Hash32) {
	// Update status of block request
	m.currentLock.Lock()
	defer m.currentLock.Unlock()

	if !hash.Equal(&m.currentHash) {
		logger.Verbose(ctx, "Block not currently active")
		return
	}

	if m.currentIsComplete {
		logger.Verbose(ctx, "Block already marked complete")
		return
	}

	m.currentIsComplete = true
	close(m.currentComplete)
	logger.Verbose(ctx, "Block marked complete")
}

type downloadFinisher struct {
	manager    *BlockManager
	downloader *BlockDownloader
}

func (c *downloadFinisher) onDownloaderCompleted(ctx context.Context, err error) {
	hash := c.downloader.Hash()
	ctx = logger.ContextWithLogFields(ctx,
		logger.Stringer("connection", c.downloader.RequesterID()),
		logger.Stringer("block_hash", hash), logger.Int("block_height", c.downloader.Height()))
	logger.Verbose(ctx, "Finishing downloader : %s", err)

	c.manager.removeDownloader(ctx, c.downloader)

	if err == nil {
		c.manager.markBlockRequestComplete(ctx, hash)
		return
	}

	if errors.Cause(err) == threads.Interrupted {
		logger.Verbose(ctx, "Block download interrupted")
		return
	}

	if errors.Cause(err) == errBlockDownloadCancelled {
		logger.Verbose(ctx, "Block download cancelled")
		return
	}

	if IsCloseError(err) {
		logger.Verbose(ctx, "Block download aborted by remote node : %s", err)
		return
	}

	logger.Warn(ctx, "Block download failed : %s", err)
}
