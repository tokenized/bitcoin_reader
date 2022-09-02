package bitcoin_reader

import (
	"context"
	"io"
	"net"
	"sync"
	"time"

	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/threads"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

const (
	ServiceFull = 0x01
)

var (
	ErrTimeout = errors.New("Timeout")
	ErrBusy    = errors.New("Busy")

	// ErrNotFullService node is not a full service node.
	ErrNotFullService = errors.New("Not Full Service")
)

// BitcoinNode is a connection to a Bitcoin node in the peer to peer network that can be used to
// send requests.
type BitcoinNode struct {
	id        uuid.UUID
	address   string
	userAgent string
	config    *Config

	headers HeaderRepository
	peers   PeerRepository

	connection              net.Conn // Connection to trusted full node
	connectionClosedLocally bool
	connectionLock          sync.Mutex
	// IP             net.IP
	// Port           uint16

	pingNonce uint64
	pingSent  time.Time

	handlers          MessageHandlers
	headerHandler     MessageHandlerFunction
	lastHeaderHash    *bitcoin.Hash32 // last header received from the node
	lastHeaderRequest []bitcoin.Hash32

	requestTime        *time.Time
	blockRequest       *bitcoin.Hash32
	blockHandler       HandleBlock
	blockReader        io.ReadCloser
	blockOnStop        OnStop
	lastRequestedBlock *bitcoin.Hash32

	txManager       *TxManager
	txReceivedCount uint64
	txReceivedSize  uint64

	outgoingMsgChannel MessageChannel
	handshakeChannel   chan wire.Message

	handshakeIsComplete bool
	isReady             bool
	isStopped           bool
	verified            bool
	protoconfCount      int

	isVerifyOnly bool // disconnect after chain verification

	sync.Mutex
}

type HeaderRepository interface {
	GetNewHeadersAvailableChannel() <-chan *wire.BlockHeader
	Height() int
	Hash(ctx context.Context, height int) (*bitcoin.Hash32, error)
	HashHeight(hash bitcoin.Hash32) int
	LastHash() bitcoin.Hash32
	LastTime() uint32
	PreviousHash(bitcoin.Hash32) (*bitcoin.Hash32, int)
	GetLocatorHashes(ctx context.Context, max int) ([]bitcoin.Hash32, error)
	GetVerifyOnlyLocatorHashes(ctx context.Context) ([]bitcoin.Hash32, error)
	VerifyHeader(ctx context.Context, header *wire.BlockHeader) error
	ProcessHeader(ctx context.Context, header *wire.BlockHeader) error
	Stop(ctx context.Context)
}

type PeerRepository interface {
	Add(ctx context.Context, address string) (bool, error)
	Get(ctx context.Context, minScore, maxScore int32) (PeerList, error)

	UpdateTime(ctx context.Context, address string) bool
	UpdateScore(ctx context.Context, address string, delta int32) bool
}

func NewBitcoinNode(address, userAgent string, config *Config, headers HeaderRepository,
	peers PeerRepository) *BitcoinNode {

	result := &BitcoinNode{
		id:               uuid.New(),
		address:          address,
		userAgent:        userAgent,
		config:           config,
		headers:          headers,
		peers:            peers,
		handlers:         make(MessageHandlers),
		handshakeChannel: make(chan wire.Message, 10),
	}

	// Only enable messages that are required for handshake and verification.
	result.handlers[wire.CmdVersion] = result.handleVersion
	result.handlers[wire.CmdVerAck] = result.handleVerack
	result.handlers[wire.CmdHeaders] = result.handleHeadersVerify
	result.handlers[wire.CmdProtoconf] = result.handleProtoconf
	result.handlers[wire.CmdPing] = result.handlePing
	result.handlers[wire.CmdReject] = result.handleReject

	// Extended messages must be handled to properly get the size of the message. The payload
	// message will still be ignored if the tx and block handlers aren't enabled.
	result.handlers[wire.CmdExtended] = result.handleExtended

	return result
}

func (n *BitcoinNode) ID() uuid.UUID {
	n.Lock()
	defer n.Unlock()

	return n.id
}

// SetVerifyOnly sets the node to only verify the correct chain and then disconnect.
func (n *BitcoinNode) SetVerifyOnly() {
	n.Lock()
	defer n.Unlock()

	n.isVerifyOnly = true
}

func (n *BitcoinNode) SetTxManager(txManager *TxManager) {
	n.Lock()
	defer n.Unlock()

	n.txManager = txManager
}

func (n *BitcoinNode) GetAndResetTxReceivedCount() (uint64, uint64) {
	n.Lock()
	defer n.Unlock()

	resultCount := n.txReceivedCount
	resultSize := n.txReceivedSize
	n.txReceivedCount = 0
	n.txReceivedSize = 0
	return resultCount, resultSize
}

func (n *BitcoinNode) IsBusy() bool {
	n.Lock()
	defer n.Unlock()

	return n.requestTime != nil
}

func (n *BitcoinNode) HasBlock(ctx context.Context, hash bitcoin.Hash32, height int) bool {
	n.Lock()
	ctx = logger.ContextWithLogFields(ctx, logger.Stringer("connection", n.id))
	lastRequestedBlock := n.lastRequestedBlock
	lastHeaderHash := n.lastHeaderHash
	n.Unlock()

	if lastRequestedBlock != nil && lastRequestedBlock.Equal(&hash) {
		logger.Verbose(ctx, "Already requested block from this node")
		return false // already requested this block and failed
	}

	if lastHeaderHash == nil {
		logger.Verbose(ctx, "No headers to check block hash")
		return false
	}

	if lastHeaderHash.Equal(&hash) {
		logger.Verbose(ctx, "Requested block is last header")
		return true
	}

	lastHeight := n.headers.HashHeight(*lastHeaderHash)
	if lastHeight == -1 {
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Stringer("last_hash", lastHeaderHash),
		}, "Last header height not found")
		return false // node's last header isn't in our chain
	}

	if lastHeight >= height {
		logger.VerboseWithFields(ctx, []logger.Field{
			logger.Int("last_height", lastHeight),
		}, "Requested block is at or below node's last header")
	} else {
		logger.VerboseWithFields(ctx, []logger.Field{
			logger.Int("last_height", lastHeight),
		}, "Requested block is above node's last header")
	}
	return lastHeight >= height
}

func (n *BitcoinNode) RequestBlock(ctx context.Context, hash bitcoin.Hash32, handler HandleBlock,
	onStop OnStop) error {
	n.Lock()
	ctx = logger.ContextWithLogFields(ctx, logger.Stringer("connection", n.id))

	if n.requestTime != nil {
		n.Unlock()
		return ErrBusy
	}

	now := time.Now()
	n.requestTime = &now
	n.blockRequest = &hash
	n.handlers[wire.CmdBlock] = n.handleBlock
	n.blockHandler = handler
	n.blockReader = nil
	n.lastRequestedBlock = &hash
	n.Unlock()

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Stringer("block_hash", hash),
		logger.Int("block_height", n.headers.HashHeight(hash)),
	}, "Requesting block")
	getBlocks := wire.NewMsgGetData() // Block request message
	getBlocks.AddInvVect(wire.NewInvVect(wire.InvTypeBlock, &hash))
	if err := n.sendMessage(ctx, getBlocks); err != nil {
		return errors.Wrap(err, "send block request")
	}

	n.Lock()
	n.blockOnStop = onStop
	n.Unlock()

	return nil
}

// CancelBlockRequest cancels a request for a block. It returns true if the block handler has
// already been called and started handling the block.
func (n *BitcoinNode) CancelBlockRequest(ctx context.Context, hash bitcoin.Hash32) bool {
	n.Lock()
	defer n.Unlock()

	if n.blockRequest == nil {
		logger.Warn(ctx, "Block request not found to cancel")
		return false
	}

	if !n.blockRequest.Equal(&hash) {
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Stringer("current_block_hash", n.blockRequest),
		}, "Wrong block request found to cancel")
		return false
	}

	if n.blockReader != nil {
		// Stop in progress handling of block
		n.blockReader.Close()
		n.blockReader = nil
		n.blockOnStop = nil
		n.blockHandler = nil
		logger.Info(ctx, "Cancelled in progress block")
		return true
	}

	// Stop handling a block before it happens
	n.blockOnStop = nil
	n.blockHandler = nil
	logger.Info(ctx, "Cancelled block request before download started")
	return false
}

func (n *BitcoinNode) RequestHeaders(ctx context.Context) error {
	n.Lock()
	ctx = logger.ContextWithLogFields(ctx, logger.Stringer("connection", n.id))

	if n.requestTime != nil {
		n.Unlock()
		return ErrBusy
	}
	n.Unlock()

	logger.Verbose(ctx, "Requesting headers")
	if err := n.sendHeaderRequest(ctx); err != nil {
		return errors.Wrap(err, "send header request")
	}

	return nil
}

func (n *BitcoinNode) RequestTxs(ctx context.Context, txids []bitcoin.Hash32) error {
	ctx = logger.ContextWithLogFields(ctx, logger.Stringer("connection", n.ID()))
	logger.Info(ctx, "Requesting %d previous txs", len(txids))

	invRequest := wire.NewMsgGetData()
	for _, txid := range txids {
		hash := txid
		item := wire.NewInvVect(wire.InvTypeTx, &hash)

		if err := invRequest.AddInvVect(item); err != nil {
			// Too many requests for one message, send it and start a new message.
			if err := n.sendMessage(ctx, invRequest); err != nil {
				return errors.Wrap(err, "send tx request")
			}

			invRequest = wire.NewMsgGetData()
			if err := invRequest.AddInvVect(item); err != nil {
				return errors.Wrap(err, "add tx to request")
			}
		}
	}

	if len(invRequest.InvList) > 0 {
		if err := n.sendMessage(ctx, invRequest); err != nil {
			return errors.Wrap(err, "send tx request")
		}
	}

	return nil
}

func (n *BitcoinNode) SetBlockHandler(handler MessageHandlerFunction) {
	n.Lock()
	defer n.Unlock()

	if handler == nil {
		delete(n.handlers, wire.CmdBlock)
	} else {
		n.handlers[wire.CmdBlock] = handler
	}
}

func (n *BitcoinNode) SetHeaderHandler(handler MessageHandlerFunction) {
	n.Lock()
	defer n.Unlock()

	n.headerHandler = handler
}

func (n *BitcoinNode) SetTxHandler(handler MessageHandlerFunction) {
	n.Lock()
	defer n.Unlock()

	if handler == nil {
		delete(n.handlers, wire.CmdTx)
	} else {
		n.handlers[wire.CmdTx] = handler
	}
}

func (n *BitcoinNode) Run(ctx context.Context, interrupt <-chan interface{}) error {
	logger.VerboseWithFields(ctx, []logger.Field{
		logger.String("address", n.address),
	}, "Connecting to node")

	if err := n.connect(ctx); err != nil {
		n.Lock()
		n.isReady = false
		n.isStopped = true
		n.Unlock()
		logger.VerboseWithFields(ctx, []logger.Field{
			logger.String("address", n.address),
		}, "Failed to connect to node : %s", err)
		return nil
	}

	n.outgoingMsgChannel.Open(1000)

	var stopper threads.StopCombiner
	var wait sync.WaitGroup

	stopper.Add(n) // close connection and outgoing channel to stop incoming and outgoing threads

	readIncomingThread, readIncomingComplete := threads.NewUninterruptableThreadComplete("Read Incoming",
		n.readIncoming, &wait)

	sendOutgoingThread, sendOutgoingComplete := threads.NewUninterruptableThreadComplete("Send Outgoing",
		n.sendOutgoing, &wait)

	pingThread, pingComplete := threads.NewPeriodicThreadComplete("Ping", n.sendPing,
		10*time.Minute, &wait)
	stopper.Add(pingThread)

	handshakeThread := threads.NewInterruptableThread("Handshake", n.handshake)
	handshakeThread.SetWait(&wait)
	stopper.Add(handshakeThread)

	// Start threads
	readIncomingThread.Start(ctx)
	sendOutgoingThread.Start(ctx)
	pingThread.Start(ctx)
	handshakeThread.Start(ctx)

	// Wait for a thread to complete
	select {
	case <-interrupt:
	case <-readIncomingComplete:
	case <-sendOutgoingComplete:
	case <-pingComplete:
	case <-time.After(n.config.Timeout.Duration):
		logger.Info(ctx, "Node reached timeout")
	}

	stopper.Stop(ctx)

	n.Lock()
	blockOnStop := n.blockOnStop
	n.isReady = false
	n.Unlock()

	if blockOnStop != nil {
		logger.Info(ctx, "Calling block request \"on stop\" function")
		waitWarning := logger.NewWaitingWarning(ctx, time.Second, "Call block \"on stop\"")
		blockOnStop(ctx)
		waitWarning.Cancel()
	}

	waitWarning := logger.NewWaitingWarning(ctx, 3*time.Second, "Node Shutdown")
	wait.Wait()
	waitWarning.Cancel()

	n.Lock()
	n.isStopped = true
	n.Unlock()

	return threads.CombineErrors(
		handshakeThread.Error(),
		readIncomingThread.Error(),
		sendOutgoingThread.Error(),
	)
}

func (n *BitcoinNode) Stop(ctx context.Context) {
	n.connectionLock.Lock()
	if n.connection != nil {
		n.connection.Close()
		n.connection = nil
		n.connectionClosedLocally = true
	}
	n.connectionLock.Unlock()

	n.outgoingMsgChannel.Close()
}

func (n *BitcoinNode) HandshakeIsComplete() bool {
	n.Lock()
	defer n.Unlock()

	return n.handshakeIsComplete
}

func (n *BitcoinNode) IsReady() bool {
	n.Lock()
	defer n.Unlock()

	return n.isReady
}

func (n *BitcoinNode) IsStopped() bool {
	n.Lock()
	defer n.Unlock()

	return n.isStopped
}

func (n *BitcoinNode) Verified() bool {
	n.Lock()
	defer n.Unlock()

	return n.verified
}

func (n *BitcoinNode) Address() string {
	n.Lock()
	defer n.Unlock()

	return n.address
}

// handshake performs the initial handshake with the node.
func (n *BitcoinNode) handshake(ctx context.Context, interrupt <-chan interface{}) error {
	versionReceived := false
	verAckSent := false
	verAckReceived := false

	n.Lock()
	address := n.address
	userAgent := n.userAgent
	receiveTxs := n.txManager != nil
	n.Unlock()

	if err := n.sendMessage(ctx, buildVersionMsg(address, userAgent, n.headers.Height(),
		receiveTxs)); err != nil {
		return errors.Wrap(err, "send version")
	}

	for {
		select {
		case msg, ok := <-n.handshakeChannel:
			if !ok {
				return nil
			}

			switch message := msg.(type) {
			case *wire.MsgVersion:
				logger.InfoWithFields(ctx, []logger.Field{
					logger.String("address", address),
					logger.String("user_agent", message.UserAgent),
					logger.Int32("protocol", message.ProtocolVersion),
					logger.Formatter("services", "%016x", message.Services),
					logger.Int32("block_height", message.LastBlock),
				}, "Version")
				versionReceived = true

				if !verAckSent {
					if err := n.sendMessage(ctx, &wire.MsgVerAck{}); err != nil {
						return errors.Wrap(err, "send ver ack")
					}
					verAckSent = true
				}

				if verAckReceived {
					return n.sendVerifyInitiation(ctx)
				}

			case *wire.MsgVerAck:
				logger.Verbose(ctx, "Version acknowledged")
				verAckReceived = true

				if versionReceived {
					return n.sendVerifyInitiation(ctx)
				}
			}

		case <-time.After(3 * time.Second):
			return ErrTimeout

		case <-interrupt:
			return nil
		}
	}
}

func (n *BitcoinNode) sendVerifyInitiation(ctx context.Context) error {
	n.Lock()
	n.handshakeIsComplete = true
	n.Unlock()

	if err := n.sendMessage(ctx, wire.NewMsgProtoconf()); err != nil {
		return errors.Wrap(err, "send protoconf")
	}

	// Send header request to check the node is on the same chain
	if err := n.sendVerifyHeaderRequest(ctx); err != nil {
		return errors.Wrap(err, "send verify header request")
	}

	return nil
}

func (n *BitcoinNode) accept(ctx context.Context) error {
	n.Lock()
	n.isReady = true
	n.verified = true

	// Switch headers handler to tracking mode.
	n.handlers[wire.CmdHeaders] = n.handleHeadersTrack

	// Enable more commands. These messages are ignored before this point.
	n.handlers[wire.CmdAddr] = n.handleAddress
	n.handlers[wire.CmdPong] = n.handlePong
	n.handlers[wire.CmdGetAddr] = n.handleGetAddresses

	// Enable tx handling
	if n.txManager != nil {
		n.handlers[wire.CmdInv] = n.handleInventory
		n.handlers[wire.CmdTx] = n.handleTx
	}

	isVerifyOnly := n.isVerifyOnly
	n.Unlock()

	if isVerifyOnly {
		logger.Info(ctx, "Disconnecting after chain verification")
		n.Stop(ctx)
		return nil
	}

	if err := n.sendMessage(ctx, wire.NewMsgSendHeaders()); err != nil {
		return errors.Wrap(err, "send \"sendheaders\" request")
	}

	if err := n.sendMessage(ctx, wire.NewMsgGetAddr()); err != nil {
		return errors.Wrap(err, "send peer request")
	}

	// Send initial header request to get any new headers the node might have.
	if err := n.sendInitialHeaderRequest(ctx); err != nil {
		return errors.Wrap(err, "send initial header request")
	}

	addresses, err := buildAddressesMessage(ctx, n.peers)
	if err != nil {
		return errors.Wrap(err, "build addresses")
	}

	logger.Info(ctx, "Sending %d addresses", len(addresses.AddrList))
	if err := n.sendMessage(ctx, addresses); err != nil {
		return errors.Wrap(err, "send addresses")
	}

	return nil
}

func (n *BitcoinNode) sendPing(ctx context.Context) error {
	n.Lock()
	defer n.Unlock()

	n.pingNonce = nonce()
	n.pingSent = time.Now()

	logger.Debug(ctx, "Sending ping 0x%16x", n.pingNonce)
	return n.sendMessage(ctx, wire.NewMsgPing(n.pingNonce))
}

func (n *BitcoinNode) connect(ctx context.Context) error {
	connection, err := net.DialTimeout("tcp", n.address, 5*time.Second)
	if err != nil {
		return err
	}

	addr := connection.RemoteAddr()
	ip, port := parseAddress(addr.String())
	if ip == nil {
		logger.Info(ctx, "Connected to unknown IP")
	} else {
		logger.Verbose(ctx, "Connected to %s:%d", ip.String(), port)
	}

	n.connectionLock.Lock()
	n.connection = connection
	n.connectionLock.Unlock()
	n.peers.UpdateTime(ctx, n.address)
	return nil
}
