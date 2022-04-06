package bitcoin_reader

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/logger"
	"github.com/tokenized/pkg/threads"
	"github.com/tokenized/pkg/wire"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

var (
	ErrNodeNotAvailable = errors.New("Node Not Available")

	errPeersNotAvailable = errors.New("Peers Not Available")
)

type NodeHasDataFunction func(context.Context, *BitcoinNode) bool

type NodeManager struct {
	userAgent     string
	config        *Config
	headers       HeaderRepository
	peers         PeerRepository
	headerHandler MessageHandlerFunction

	lastHeaderRequest time.Time
	lastNewHeader     time.Time

	nodes          []*nodeThread
	nextNodeOffset int // offset of node for next request

	scanNodes     []*nodeThread
	scanningPeers map[string]time.Time // Peer addresses currenctly being scanned

	previousPeers map[string]time.Time // Recently used peer addresses
	peersLists    map[int]PeerList

	txManager        *TxManager
	blockManager     *BlockManager
	blockTxManager   BlockTxManager
	blockTxProcessor TxProcessor

	blockManagerLock   sync.Mutex
	blockSyncNeeded    bool
	blockManagerThread *threads.Thread

	initialDelayComplete bool
	inSync               bool

	syncBlocksWait sync.WaitGroup
	wait           sync.WaitGroup
	sync.Mutex
}

type nodeThread struct {
	node   *BitcoinNode
	thread *threads.Thread
	id     uuid.UUID
}

func NewNodeManager(userAgent string, config *Config, headers HeaderRepository,
	peers PeerRepository) *NodeManager {

	result := &NodeManager{
		userAgent:         userAgent,
		config:            config,
		headers:           headers,
		peers:             peers,
		lastHeaderRequest: time.Now(),
		lastNewHeader:     time.Now(),
		scanningPeers:     make(map[string]time.Time),
		previousPeers:     make(map[string]time.Time),
		peersLists:        make(map[int]PeerList),
	}

	return result
}

func (m *NodeManager) SetTxManager(txManager *TxManager) {
	m.Lock()
	defer m.Unlock()

	m.txManager = txManager
}

func (m *NodeManager) SetBlockManager(blockTxManager BlockTxManager, blockManager *BlockManager,
	txProcessor TxProcessor) {
	m.Lock()
	defer m.Unlock()

	m.blockTxManager = blockTxManager
	m.blockManager = blockManager
	m.blockTxProcessor = txProcessor
}

func (m *NodeManager) SetHeaderHandler(headerHandler MessageHandlerFunction) {
	m.Lock()
	defer m.Unlock()

	m.headerHandler = headerHandler
}

func (m *NodeManager) RequestBlock(ctx context.Context, hash bitcoin.Hash32,
	handler HandleBlock, onStop OnStop) (BlockRequestCanceller, error) {
	m.Lock()
	defer m.Unlock()

	height := m.headers.HashHeight(hash)
	if height == -1 {
		return nil, fmt.Errorf("Block not in headers : %s", hash)
	}

	ctx = logger.ContextWithLogFields(ctx, logger.String("task", "Request Block"),
		logger.Stringer("block_hash", hash), logger.Int("block_height", height))

	// inline function to access "hash" and "height" without a parameter in the predefined "hasData"
	// function.
	nodeHasBlock := func(ctx context.Context, node *BitcoinNode) bool {
		return node.HasBlock(ctx, hash, height)
	}

	node := m.nextNode(ctx, nodeHasBlock)
	if node == nil {
		return nil, ErrNodeNotAvailable
	}

	return node, node.RequestBlock(ctx, hash, handler, onStop)
}

func (m *NodeManager) RequestHeaders(ctx context.Context) error {
	m.Lock()
	defer m.Unlock()

	ctx = logger.ContextWithLogFields(ctx, logger.String("task", "Request Headers"))

	node := m.nextNode(ctx, nil)
	if node == nil {
		return nil
	}

	return node.RequestHeaders(ctx)
}

func (m *NodeManager) RequestTxs(ctx context.Context) error {
	m.Lock()
	defer m.Unlock()

	if m.txManager == nil {
		return nil
	}

	ctx = logger.ContextWithLogFields(ctx, logger.String("task", "Request Transactions"))

	node := m.nextNode(ctx, nil)
	if node == nil {
		return nil
	}

	txids, err := m.txManager.GetTxRequests(ctx, node.id, m.config.TxRequestCount)
	if err != nil {
		return errors.Wrap(err, "get tx requests")
	}

	if len(txids) == 0 {
		return nil
	}

	return node.RequestTxs(ctx, txids)
}

func (m *NodeManager) SendTx(ctx context.Context, tx *wire.MsgTx) error {
	m.Lock()
	defer m.Unlock()

	count := 0
	for _, node := range m.nodes {
		if !node.node.IsReady() {
			node.node.sendMessage(ctx, tx)
			count++
		}
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("txid", tx.TxHash()),
		logger.Int("node_count", count),
	}, "Sent tx to nodes")
	return nil
}

func (m *NodeManager) nextNode(ctx context.Context, hasData NodeHasDataFunction) *BitcoinNode {
	if len(m.nodes) == 0 {
		return nil
	}

	var stoppedNodes []fmt.Stringer
	var notReadyNodes []fmt.Stringer
	var busyNodes []fmt.Stringer
	looped := false
	for {
		if m.nextNodeOffset >= len(m.nodes) {
			if looped {
				logger.WarnWithFields(ctx, []logger.Field{
					logger.Stringers("stopped", stoppedNodes),
					logger.Stringers("not_ready", notReadyNodes),
					logger.Stringers("busy", busyNodes),
				}, "No nodes available")
				return nil
			}
			m.nextNodeOffset = 0
			looped = true
		}

		if m.nodes[m.nextNodeOffset].node.IsStopped() {
			stoppedNodes = append(stoppedNodes, m.nodes[m.nextNodeOffset].node.ID())
			m.nodes = append(m.nodes[:m.nextNodeOffset], m.nodes[m.nextNodeOffset+1:]...)
			continue
		}

		if !m.nodes[m.nextNodeOffset].node.IsReady() {
			notReadyNodes = append(notReadyNodes, m.nodes[m.nextNodeOffset].node.ID())
			m.nextNodeOffset++
			continue
		}

		if m.nodes[m.nextNodeOffset].node.IsBusy() {
			busyNodes = append(busyNodes, m.nodes[m.nextNodeOffset].node.ID())
			m.nextNodeOffset++
			continue
		}

		if hasData != nil && !hasData(ctx, m.nodes[m.nextNodeOffset].node) {
			m.nextNodeOffset++
			continue
		}

		logger.VerboseWithFields(ctx, []logger.Field{
			logger.Stringers("stopped", stoppedNodes),
			logger.Stringers("not_ready", notReadyNodes),
			logger.Stringers("busy", busyNodes),
			logger.Stringer("available", m.nodes[m.nextNodeOffset].node.ID()),
		}, "Node available")
		result := m.nodes[m.nextNodeOffset].node
		m.nextNodeOffset++
		return result
	}
}

func (m *NodeManager) Run(ctx context.Context, interrupt <-chan interface{}) error {
	defer func() {
		waitWarning := logger.NewWaitingWarning(ctx, 3*time.Second,
			"Node Manager Sync Blocks Shutdown")
		m.syncBlocksWait.Wait()
		waitWarning.Cancel()

		waitWarning = logger.NewWaitingWarning(ctx, 3*time.Second, "Node Manager Shutdown")
		m.wait.Wait()
		waitWarning.Cancel()
	}()

	var resultErr error
	if err := m.FindByScore(ctx, 1, m.config.DesiredNodeCount/2); err != nil {
		if errors.Cause(err) == errPeersNotAvailable {
			logger.Info(ctx, "No peers with score 1. Finding peers with score 0")
			if err := m.FindByScore(ctx, 0, m.config.DesiredNodeCount/2); err != nil {
				resultErr = errors.Wrap(err, "find")
			}
		} else {
			resultErr = errors.Wrap(err, "find")
		}
	}

	if resultErr == nil {
		if err := m.Scan(ctx); err != nil &&
			errors.Cause(err) != errPeersNotAvailable {
			resultErr = errors.Wrap(err, "scan")
		}
	}

	if resultErr != nil {
		m.Stop(ctx)
		return resultErr
	}

	if uint32(time.Now().Unix())-m.headers.LastTime() < 3600 {
		logger.Info(ctx, "In Sync")
		m.inSync = true
	}

	var stopper threads.StopCombiner
	stopper.Add(m)

	monitorHeadersThread := threads.NewThreadWithoutStop("Monitor Headers", m.MonitorHeaders)
	monitorHeadersThread.SetWait(&m.wait)
	monitorHeadersComplete := monitorHeadersThread.GetCompleteChannel()

	checkHeadersThread := threads.NewPeriodicTask("Check Headers", 5*time.Second, m.CheckHeaders)
	checkHeadersThread.SetWait(&m.wait)
	checkHeadersComplete := checkHeadersThread.GetCompleteChannel()
	stopper.Add(checkHeadersThread)

	cleanThread := threads.NewPeriodicTask("Clean Nodes", 5*time.Second, m.Clean)
	cleanThread.SetWait(&m.wait)
	cleanComplete := cleanThread.GetCompleteChannel()
	stopper.Add(cleanThread)

	statusThread := threads.NewPeriodicTask("Status", 5*time.Minute, m.Status)
	statusThread.SetWait(&m.wait)
	statusComplete := statusThread.GetCompleteChannel()
	stopper.Add(statusThread)

	requestTxsThread := threads.NewPeriodicTask("Request Txs", 5*time.Second, m.RequestTxs)
	requestTxsThread.SetWait(&m.wait)
	requestTxsComplete := requestTxsThread.GetCompleteChannel()
	stopper.Add(requestTxsThread)

	findThread := threads.NewPeriodicTask("Find Nodes", 30*time.Second, m.Find)
	findThread.SetWait(&m.wait)
	findComplete := findThread.GetCompleteChannel()
	stopper.Add(findThread)

	scanThread := threads.NewPeriodicTask("Scan Nodes", 10*time.Minute, m.Scan)
	scanThread.SetWait(&m.wait)
	scanComplete := scanThread.GetCompleteChannel()
	stopper.Add(scanThread)

	startupDelayThread := threads.NewThread("Startup Delay", func(ctx context.Context,
		interrupt <-chan interface{}) error {

		select {
		case <-interrupt:
			return nil
		case <-time.After(m.config.StartupDelay.Duration):
			m.markStartupDelayComplete(ctx)
			return nil
		}
	})
	startupDelayThread.SetWait(&m.wait)
	stopper.Add(startupDelayThread)

	monitorHeadersThread.Start(ctx)
	checkHeadersThread.Start(ctx)
	cleanThread.Start(ctx)
	statusThread.Start(ctx)
	requestTxsThread.Start(ctx)
	findThread.Start(ctx)
	scanThread.Start(ctx)
	startupDelayThread.Start(ctx)

	// Wait for interrupt or a thread to stop
	select {
	case <-monitorHeadersComplete:
		logger.Warn(ctx, "Finished: Monitor Headers")

	case <-checkHeadersComplete:
		logger.Warn(ctx, "Finished: Check Headers")

	case <-cleanComplete:
		logger.Warn(ctx, "Finished: Clean")

	case <-statusComplete:
		logger.Warn(ctx, "Finished: Status")

	case <-requestTxsComplete:
		logger.Warn(ctx, "Finished: Request Txs")

	case <-findComplete:
		logger.Warn(ctx, "Finished: Find")

	case <-scanComplete:
		logger.Warn(ctx, "Finished: Scan")

	case <-interrupt:
	}

	// Stop the remaining threads. This also calls NodeManager.Stop.
	stopper.Stop(ctx)

	return threads.CombineErrors(
		monitorHeadersThread.Error(),
		checkHeadersThread.Error(),
		cleanThread.Error(),
		statusThread.Error(),
		requestTxsThread.Error(),
		findThread.Error(),
		scanThread.Error(),
	)
}

func (m *NodeManager) Stop(ctx context.Context) {
	m.Lock()
	defer m.Unlock()

	m.headers.Stop(ctx)

	for _, node := range m.nodes {
		node.thread.Stop(ctx)
	}

	for _, node := range m.scanNodes {
		node.thread.Stop(ctx)
	}

	m.blockManagerLock.Lock()
	if m.blockManagerThread != nil {
		if !m.blockManagerThread.IsComplete() {
			m.blockSyncNeeded = false
			m.blockManagerThread.Stop(ctx)
		}
	}
	m.blockManagerLock.Unlock()
}

func (m *NodeManager) markStartupDelayComplete(ctx context.Context) {
	logger.Info(ctx, "Startup delay complete")
	m.Lock()
	m.initialDelayComplete = true
	m.Unlock()

	m.TriggerBlockSynchronize(ctx)
}

func (m *NodeManager) TriggerBlockSynchronize(ctx context.Context) {
	m.blockManagerLock.Lock()
	defer m.blockManagerLock.Unlock()

	if m.blockManager == nil {
		return
	}

	m.Lock()
	if !m.initialDelayComplete {
		m.Unlock()
		return
	}
	m.Unlock()

	if m.blockManagerThread != nil {
		if m.blockManagerThread.IsComplete() {
			m.blockManagerThread = nil
		}
	}

	if m.blockManagerThread != nil {
		if !m.blockSyncNeeded {
			logger.Info(ctx, "Setting flag to restart block synchronization after current round")
			m.blockSyncNeeded = true
		}
		return
	}

	// Start a new thread
	m.blockManagerThread = threads.NewThread("Synchronize Blocks", m.runSynchronizeBlocks)
	m.Lock()
	m.blockManagerThread.SetWait(&m.syncBlocksWait)
	m.Unlock()
	m.blockManagerThread.Start(ctx)
}

func (m *NodeManager) runSynchronizeBlocks(ctx context.Context,
	interrupt <-chan interface{}) error {

	for {
		if err := m.synchronizeBlocks(ctx, interrupt); err != nil {
			if errors.Cause(err) == threads.Interrupted {
				return nil
			}
			return err
		}

		m.blockManagerLock.Lock()
		blockSyncNeeded := m.blockSyncNeeded
		m.blockSyncNeeded = false
		m.blockManagerLock.Unlock()

		if !blockSyncNeeded {
			return nil
		}

		logger.Info(ctx, "Restarting block synchronization")
	}
}

func (m *NodeManager) synchronizeBlocks(ctx context.Context, interrupt <-chan interface{}) error {
	m.blockManagerLock.Lock()
	blockManager := m.blockManager
	m.blockManagerLock.Unlock()

	if blockManager == nil {
		return nil
	}

	// Find oldest block that needs processed
	lashHash := m.headers.LastHash()
	lastHeight := m.headers.HashHeight(lashHash)
	if lastHeight == -1 {
		return nil // most POW chain already moved again
	}
	if lastHeight < m.config.StartBlockHeight {
		return nil // not at start height yet
	}

	hash := lashHash
	height := lastHeight
	if _, exists, err := m.blockTxManager.FetchBlockTxIDs(ctx, hash); err != nil {
		return errors.Wrap(err, "fetch block txids")
	} else if exists {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("block_hash", hash),
			logger.Int("block_height", height),
		}, "Blocks in sync")
		return nil
	}

	hashes := []bitcoin.Hash32{hash}
	for {
		// Get previous header hash
		previousHash, _ := m.headers.PreviousHash(hash)
		if previousHash == nil {
			return nil // headers must have reorged
		}

		// Check if block has already been processed
		if _, exists, err := m.blockTxManager.FetchBlockTxIDs(ctx, *previousHash); err != nil {
			return errors.Wrap(err, "fetch block txids")
		} else if exists {
			logger.InfoWithFields(ctx, []logger.Field{
				logger.Stringer("block_hash", previousHash),
				logger.Int("block_height", height-1),
			}, "Found processed block")
			break // block already processed
		}

		hash = *previousHash
		hashes = append([]bitcoin.Hash32{*previousHash}, hashes...)
		height--

		if height <= m.config.StartBlockHeight {
			logger.InfoWithFields(ctx, []logger.Field{
				logger.Stringer("block_hash", hash),
				logger.Int("block_height", height),
			}, "Reached start block height")
			break
		}
	}

	startHash := hash
	startHeight := height

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("start_block_hash", startHash),
		logger.Int("start_block_height", startHeight),
		logger.Int("block_count", len(hashes)),
		logger.Stringer("last_block_hash", lashHash),
		logger.Int("last_block_height", lastHeight),
	}, "Processing blocks")

	for _, hash := range hashes {
		complete := blockManager.AddRequest(ctx, hash, height, m.blockTxProcessor)

		select {
		case <-complete:
		case <-interrupt:
			return threads.Interrupted
		}

		height++
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("start_block_hash", startHash),
		logger.Int("start_block_height", startHeight),
		logger.Int("block_count", len(hashes)),
		logger.Stringer("last_block_hash", lashHash),
		logger.Int("last_block_height", lastHeight),
	}, "Finished processing blocks")

	return nil
}

func (m *NodeManager) MonitorHeaders(ctx context.Context) error {
	headersChannel := m.headers.GetNewHeadersAvailableChannel()

	lastRequest := time.Now()
	countSinceLastRequest := 0
	for {
		select {
		case header, ok := <-headersChannel:
			if !ok {
				return nil
			}

			m.Lock()
			if !m.inSync {
				age := uint32(time.Now().Unix()) - header.Timestamp
				if age < 3600 {
					logger.Info(ctx, "In Sync")
					m.inSync = true
				}
			}
			inSync := m.inSync
			m.Unlock()

			now := time.Now()
			countSinceLastRequest++
			if now.Sub(lastRequest).Seconds() > 0.5 || countSinceLastRequest > 1000 {
				if err := m.RequestHeaders(ctx); err != nil &&
					errors.Cause(err) != errPeersNotAvailable {
					logger.Warn(ctx, "Failed to request headers : %s", err)
				}
				m.Lock()
				m.lastNewHeader = now
				m.Unlock()
				lastRequest = now
				countSinceLastRequest = 0
			}

			if inSync {
				m.TriggerBlockSynchronize(ctx)
			}
		}
	}
}

func (m *NodeManager) CheckHeaders(ctx context.Context) error {
	m.Lock()
	lastHeaderRequest := m.lastHeaderRequest
	lastNewHeader := m.lastNewHeader
	m.Unlock()

	sinceRequest := time.Since(lastHeaderRequest).Seconds()
	sinceNew := time.Since(lastNewHeader).Seconds()

	if sinceRequest > 60.0 || (sinceNew < 5.0 && sinceRequest > 2.0) {
		if err := m.RequestHeaders(ctx); err != nil &&
			errors.Cause(err) != errPeersNotAvailable {
			return err
		}
		m.Lock()
		m.lastHeaderRequest = time.Now()
		m.Unlock()
	}

	return nil
}

func (m *NodeManager) Clean(ctx context.Context) error {
	m.Lock()
	defer m.Unlock()

	var newNodes []*nodeThread
	for _, node := range m.nodes {
		if node.node.IsStopped() && node.thread.IsComplete() {
			if err := node.thread.Error(); err != nil {
				m.peers.UpdateScore(ctx, node.node.Address(), -1)
			} else {
				if node.node.Verified() {
					m.peers.UpdateScore(ctx, node.node.Address(), 1)
				} else {
					m.peers.UpdateScore(ctx, node.node.Address(), -1)
				}
			}
			continue
		}

		newNodes = append(newNodes, node)
	}
	m.nodes = newNodes

	var newScanNodes []*nodeThread
	scanFound := 0
	for _, node := range m.scanNodes {
		if node.node.IsStopped() && node.thread.IsComplete() {
			if err := node.thread.Error(); err != nil {
				m.peers.UpdateScore(ctx, node.node.Address(), -1)
			} else {
				if node.node.Verified() {
					scanFound++
					m.peers.UpdateScore(ctx, node.node.Address(), 1)
				} else {
					m.peers.UpdateScore(ctx, node.node.Address(), -1)
				}
			}
			continue
		}

		newScanNodes = append(newScanNodes, node)
	}
	m.scanNodes = newScanNodes

	if scanFound > 0 {
		logger.Info(ctx, "Scan found %d peers", scanFound)
	}

	return nil
}

func (m *NodeManager) Status(ctx context.Context) error {
	readyCount := 0
	totalCount := 0
	txCount := uint64(0)
	txSize := uint64(0)

	m.Lock()
	for _, node := range m.nodes {
		if node.thread.IsComplete() {
			continue
		}

		totalCount++
		if node.node.IsReady() {
			readyCount++

			nodeCount, nodeSize := node.node.GetAndResetTxReceivedCount()
			txCount += nodeCount
			txSize += nodeSize
		}
	}
	m.Unlock()

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Int("nodes_ready", readyCount),
		logger.Int("nodes_total", totalCount),
		logger.Int("nodes_desired", m.config.DesiredNodeCount),
	}, "Node summary")

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Uint64("tx_count", txCount),
		logger.Float64("tx_size_mb", float64(txSize)/1e6),
	}, "Tx summary")
	return nil
}

func (m *NodeManager) Find(ctx context.Context) error {
	if err := m.FindByScore(ctx, 5, m.config.DesiredNodeCount/4); err != nil &&
		errors.Cause(err) != errPeersNotAvailable {
		return err
	}

	if err := m.FindByScore(ctx, 1, m.config.DesiredNodeCount/2); err != nil {
		if errors.Cause(err) == errPeersNotAvailable {
			logger.Info(ctx, "Peers not available")
			if err := m.Scan(ctx); err != nil &&
				errors.Cause(err) != errPeersNotAvailable {
				return err
			}
		} else {
			return err
		}
	}

	return nil
}

func (m *NodeManager) getPeers(ctx context.Context, score int) (PeerList, error) {
	list := m.peersLists[score]
	if len(list) == 0 {
		maxScore := -1
		if score == 1 {
			maxScore = 4
		}
		peers, err := m.peers.Get(ctx, int32(score), int32(maxScore))
		if err != nil {
			return nil, errors.Wrap(err, "get peers")
		}
		list = peers
		m.peersLists[score] = list
		if maxScore == -1 {
			logger.VerboseWithFields(ctx, []logger.Field{
				logger.Int("peer_count", len(list)),
			}, "Retrieved peers for score range %d and up", score)
		} else {
			logger.VerboseWithFields(ctx, []logger.Field{
				logger.Int("peer_count", len(list)),
			}, "Retrieved peers for score range %d to %d", score, maxScore)
		}
	}

	return list, nil
}

func (m *NodeManager) FindByScore(ctx context.Context, score, max int) error {
	m.Lock()
	defer m.Unlock()

	if len(m.nodes) >= m.config.DesiredNodeCount {
		return nil
	}

	peers, err := m.getPeers(ctx, score)
	if err != nil {
		return errors.Wrap(err, "get peers")
	}

	logger.VerboseWithFields(ctx, []logger.Field{
		logger.Int("peer_count", len(peers)),
	}, "Peers for score %d", score)

	newNodes := 0
	offset := 0
	for _, peer := range peers {
		offset++

		if previousTime, exists := m.previousPeers[peer.Address]; exists &&
			time.Since(previousTime) < m.config.Timeout.Duration {
			logger.DebugWithFields(ctx, []logger.Field{
				logger.String("address", peer.Address),
				logger.Timestamp("previous_time", previousTime.UnixNano()),
			}, "Skipping recent peer")
			continue
		}

		if previousTime, exists := m.scanningPeers[peer.Address]; exists &&
			time.Since(previousTime) < time.Minute {
			logger.DebugWithFields(ctx, []logger.Field{
				logger.String("address", peer.Address),
				logger.Timestamp("previous_time", previousTime.UnixNano()),
			}, "Skipping scanning peer")
			continue
		}

		node := NewBitcoinNode(peer.Address, m.userAgent, m.config, m.headers, m.peers)
		if m.headerHandler != nil {
			node.SetHeaderHandler(m.headerHandler)
		}
		if m.txManager != nil {
			node.SetTxManager(m.txManager)
		}

		nodeCtx := logger.ContextWithLogFields(ctx, logger.Stringer("connection", node.ID()))
		thread := threads.NewThread(fmt.Sprintf("Node: %s", peer.Address), node.Run)
		thread.SetWait(&m.wait)
		thread.Start(nodeCtx)
		m.previousPeers[peer.Address] = time.Now()

		m.nodes = append(m.nodes, &nodeThread{
			node:   node,
			thread: thread,
			id:     node.ID(),
		})
		newNodes++

		if newNodes >= max {
			break
		}
	}

	// Remove used peers
	m.peersLists[score] = peers[offset:]

	if newNodes == 0 {
		return errPeersNotAvailable
	}

	return nil
}

func (m *NodeManager) getScanPeers(ctx context.Context) (PeerList, error) {
	list := m.peersLists[-1]
	if len(list) == 0 {
		peers, err := m.peers.Get(ctx, -5, 0)
		if err != nil {
			return nil, errors.Wrap(err, "get peers")
		}
		list = peers
		m.peersLists[-1] = list
		logger.VerboseWithFields(ctx, []logger.Field{
			logger.Int("peer_count", len(list)),
		}, "Retrieved peers for scanning")
	}

	return list, nil
}

func (m *NodeManager) Scan(ctx context.Context) error {
	m.Lock()
	defer m.Unlock()

	peers, err := m.getScanPeers(ctx)
	if err != nil {
		return errors.Wrap(err, "get peers")
	}

	logger.VerboseWithFields(ctx, []logger.Field{
		logger.Int("peer_count", len(peers)),
	}, "Peers for scanning")

	newNodes := 0
	offset := 0
	for _, peer := range peers {
		offset++

		if previousTime, exists := m.previousPeers[peer.Address]; exists &&
			time.Since(previousTime) < m.config.Timeout.Duration {
			continue
		}

		if previousTime, exists := m.scanningPeers[peer.Address]; exists &&
			time.Since(previousTime) < m.config.Timeout.Duration {
			continue
		}

		node := NewBitcoinNode(peer.Address, m.userAgent, m.config, m.headers, m.peers)
		node.SetVerifyOnly()

		nodeCtx := logger.ContextWithLogFields(ctx, logger.Stringer("scan_connection", node.ID()))
		thread := threads.NewThread(fmt.Sprintf("Scan Node: %s", peer.Address), node.Run)
		thread.SetWait(&m.wait)
		thread.Start(nodeCtx)
		m.scanningPeers[peer.Address] = time.Now()

		m.scanNodes = append(m.scanNodes, &nodeThread{
			node:   node,
			thread: thread,
			id:     node.ID(),
		})
		newNodes++

		if newNodes >= m.config.ScanCount {
			break
		}
	}

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Int("new_nodes", newNodes),
		logger.Int("scan_count", m.config.ScanCount),
	}, "Scanning nodes")

	// Remove used peers
	m.peersLists[-1] = peers[offset:]

	return nil
}
