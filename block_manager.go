package bitcoin_reader

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/logger"
	"github.com/tokenized/pkg/threads"

	"github.com/pkg/errors"
)

var (
	ErrWrongBlock = errors.New("Wrong Block")
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
	currentComplete   chan error
	currentLock       sync.Mutex

	wait sync.WaitGroup
	sync.Mutex
}

type downloadThread struct {
	downloader *BlockDownloader
	thread     *threads.Thread
}

type downloadRequest struct {
	hash      bitcoin.Hash32
	height    int
	processor TxProcessor
	complete  chan error
}

func NewBlockManager(blockTxManager BlockTxManager, requestor BlockRequestor,
	concurrentBlockRequests int, blockRequestDelay time.Duration) *BlockManager {

	return &BlockManager{
		blockTxManager:          blockTxManager,
		requestor:               requestor,
		concurrentBlockRequests: concurrentBlockRequests,
		blockRequestDelay:       blockRequestDelay,
		currentComplete:         make(chan error),
		requests:                make(chan *downloadRequest, 10),
	}
}

func (m *BlockManager) AddRequest(ctx context.Context, hash bitcoin.Hash32, height int,
	processor TxProcessor) <-chan error {

	request := &downloadRequest{
		hash:      hash,
		height:    height,
		processor: processor,
		complete:  make(chan error),
	}

	m.requestLock.Lock()
	if m.requestsClosed {
		m.requestLock.Unlock()
		return nil
	}
	m.requests <- request
	m.requestLock.Unlock()

	return request.complete
}

func (m *BlockManager) Run(ctx context.Context, interrupt <-chan interface{}) error {
	defer func() {
		m.Stop(ctx)
		waitWarning := logger.NewWaitingWarning(ctx, 3*time.Second, "Block Manager Shutdown")
		m.wait.Wait()
		waitWarning.Cancel()
	}()

	blockInterrupt := make(chan interface{})
	go func() {
		select {
		case <-interrupt:
			close(blockInterrupt)

			m.requestLock.Lock()
			m.requestsClosed = true
			close(m.requests)
			m.requestLock.Unlock()
		}
	}()

	for request := range m.requests {
		if err := m.processRequest(ctx, request, blockInterrupt); err != nil {
			for range m.requests { // flush channel
			}

			if errors.Cause(err) == threads.Interrupted {
				return nil
			}

			logger.Error(ctx, "Failed to process request : %s", err)
			return err
		}
	}

	return nil
}

func (m *BlockManager) processRequest(ctx context.Context, request *downloadRequest,
	interrupt <-chan interface{}) error {

	ctx = logger.ContextWithLogFields(ctx,
		logger.Stringer("block_hash", request.hash),
		logger.Int("block_height", request.height))

	m.currentLock.Lock()
	m.currentHash = request.hash
	m.currentIsComplete = false
	m.currentLock.Unlock()

	// Send initial block request.
	countSinceRequest := 0
	if err := m.requestBlock(ctx, request.hash, request.height, request.processor); err != nil {
		logger.Warn(ctx, "Failed to request block : %s", err)
	}

	// Wait for a download to complete and send new block requests as necessary.
	for {
		select {
		case <-time.After(m.blockRequestDelay): // most blocks finish within 5 seconds
			activeDownloadCount := m.DownloaderCount(request.hash)
			logger.VerboseWithFields(ctx, []logger.Field{
				logger.Int("active_downloads", activeDownloadCount),
			}, "Active downloads")
			countSinceRequest++

			if activeDownloadCount < m.concurrentBlockRequests {
				if err := m.requestBlock(ctx, request.hash, request.height,
					request.processor); err != nil {
					logger.Warn(ctx, "Failed to request block : %s", err)
				} else {
					activeDownloadCount++
					countSinceRequest = 0
				}
			}

			if countSinceRequest > 20 && activeDownloadCount < m.concurrentBlockRequests {
				return ErrNodeNotAvailable
			}

		case <-interrupt:
			return threads.Interrupted

		case err := <-m.currentComplete:
			logger.Verbose(ctx, "Completed block")
			m.stopBlock(ctx, request.hash) // stop any others still downloading
			request.complete <- err
			return nil
		}
	}
}

func (m *BlockManager) DownloaderCount(hash bitcoin.Hash32) int {
	result := 0
	m.downloaderLock.Lock()
	for _, d := range m.downloaders {
		dh := d.downloader.Hash()
		if hash.Equal(&dh) {
			result++
		}
	}
	m.downloaderLock.Unlock()
	return result
}

func (m *BlockManager) Stop(ctx context.Context) {
	m.downloaderLock.Lock()
	logger.InfoWithFields(ctx, []logger.Field{
		logger.Int("downloader_count", len(m.downloaders)),
	}, "Stopping downloaders")
	for _, d := range m.downloaders {
		d.downloader.Cancel(ctx)
		d.thread.Stop(ctx)
	}
	m.downloaderLock.Unlock()
}

func (m *BlockManager) stopBlock(ctx context.Context, hash bitcoin.Hash32) {
	m.downloaderLock.Lock()
	for _, d := range m.downloaders {
		dh := d.downloader.Hash()
		if hash.Equal(&dh) {
			d.downloader.Cancel(ctx)
			d.thread.Stop(ctx)
		}
	}
	m.downloaderLock.Unlock()
}

func (m *BlockManager) requestBlock(ctx context.Context, hash bitcoin.Hash32, height int,
	processor TxProcessor) error {

	downloader := NewBlockDownloader(processor, m.blockTxManager, hash, height,
		m.DownloaderCompleted)

	node, err := m.requestor.RequestBlock(ctx, hash, downloader.HandleBlock, downloader.Stop)
	if err != nil {
		return err
	}

	downloader.SetCanceller(node.ID(), node)

	dt := &downloadThread{
		downloader: downloader,
		thread:     threads.NewThread(fmt.Sprintf("Download Block: %s", hash), downloader.Run),
	}
	dt.thread.SetWait(&m.wait)

	m.downloaderLock.Lock()
	m.downloaders = append(m.downloaders, dt)
	m.downloaderLock.Unlock()

	// Start download thread
	dt.thread.Start(ctx)
	return nil
}

func (m *BlockManager) DownloaderCompleted(ctx context.Context, downloader *BlockDownloader,
	err error) {

	m.downloaderLock.Lock()
	for i, dt := range m.downloaders {
		if dt.downloader == downloader {
			m.downloaders = append(m.downloaders[:i], m.downloaders[i+1:]...)
			break
		}
	}
	m.downloaderLock.Unlock()

	hash := downloader.Hash()

	// Update status of block
	if err == nil {
		m.currentLock.Lock()
		if hash.Equal(&m.currentHash) {
			if !m.currentIsComplete {
				m.currentIsComplete = true
				m.currentComplete <- nil
			}
		}
		m.currentLock.Unlock()
		return
	}

	if errors.Cause(err) == threads.Interrupted || errors.Cause(err) == errBlockDownloadCancelled {
		return
	}

	logger.WarnWithFields(ctx, []logger.Field{
		logger.Stringer("block_hash", downloader.Hash()),
		logger.Int("block_height", downloader.Height()),
	}, "Block download failed with error : %s", err)
}

func (m *BlockManager) CancelDownloaders(ctx context.Context, hash bitcoin.Hash32) {
	m.downloaderLock.Lock()
	defer m.downloaderLock.Unlock()

	for _, dt := range m.downloaders {
		hash := dt.downloader.Hash()
		if hash.Equal(&hash) {
			dt.downloader.Cancel(ctx)
			dt.thread.Stop(ctx)
		}
	}
}
