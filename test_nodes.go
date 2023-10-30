package bitcoin_reader

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tokenized/bitcoin_reader/headers"
	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/threads"

	"github.com/pkg/errors"
)

const (
	TestUserAgent = "TestAgent/1"
	TestNet       = bitcoin.MainNet
)

type MockNode struct {
	Name         string
	InternalNode *BitcoinNode
	ExternalNode *MockExternalNode
}

func NewMockNode(name string, config *Config, headers *headers.Repository,
	peers PeerRepository, txManager *TxManager) *MockNode {
	result := &MockNode{
		Name:         name,
		InternalNode: NewBitcoinNode(name, TestUserAgent, config, headers, peers),
		ExternalNode: NewMockExternalNode(name, headers, 500),
	}

	result.InternalNode.SetTxManager(txManager)
	return result
}

func (n *MockNode) Run(ctx context.Context, interrupt <-chan interface{}) error {
	// Create mock connection
	incomingConn, externalConn := net.Pipe()

	n.InternalNode.mockConnect(ctx, incomingConn)
	n.ExternalNode.SetConnection(externalConn)

	var wait sync.WaitGroup

	internalThread, internalComplete := threads.NewInterruptableThreadComplete(fmt.Sprintf("%s: Internal", n.Name),
		n.InternalNode.run, &wait)
	externalThread, externalComplete := threads.NewInterruptableThreadComplete(fmt.Sprintf("%s: External", n.Name),
		n.ExternalNode.Run, &wait)

	internalThread.Start(ctx)
	externalThread.Start(ctx)

	select {
	case internalErr := <-internalComplete:
		logger.Fatal(ctx, "[%s] Internal node failed : %s", n.Name, internalErr)

	case externalErr := <-externalComplete:
		logger.Fatal(ctx, "[%s] External node failed : %s", n.Name, externalErr)

	case <-interrupt:
	}

	internalThread.Stop(ctx)
	externalThread.Stop(ctx)

	wait.Wait()
	return nil
}

type MockExternalNode struct {
	connection net.Conn
	isClosed   atomic.Value

	Name string

	Txs              []*wire.MsgTx
	TxRetentionCount int
	TxLock           sync.Mutex

	headers *headers.Repository

	outgoingMsgChannel chan wire.Message
}

func NewMockExternalNode(name string, headers *headers.Repository,
	txRetentionCount int) *MockExternalNode {
	return &MockExternalNode{
		Name:               name,
		TxRetentionCount:   txRetentionCount,
		headers:            headers,
		outgoingMsgChannel: make(chan wire.Message, 100),
	}
}

func (n *MockExternalNode) ProvideTx(tx *wire.MsgTx) error {
	n.TxLock.Lock()
	n.Txs = append(n.Txs, tx)
	if len(n.Txs) > n.TxRetentionCount {
		overage := len(n.Txs) - n.TxRetentionCount
		n.Txs = n.Txs[overage:]
	}
	n.TxLock.Unlock()

	// TODO Remove transactions if they aren't immediately requested with a get data message. --ce

	// Send tx inventory.
	msg := &wire.MsgInv{
		InvList: []*wire.InvVect{
			{
				Type: wire.InvTypeTx,
				Hash: *tx.TxHash(),
			},
		},
	}

	if err := n.sendMessage(msg); err != nil {
		return errors.Wrap(err, "send message")
	}

	return nil
}

func (n *MockExternalNode) SetConnection(connection net.Conn) {
	n.connection = connection
	n.isClosed.Store(false)
}

func (n *MockExternalNode) Run(ctx context.Context, interrupt <-chan interface{}) error {
	var wait sync.WaitGroup

	// Listen for requests from connection and respond.
	handleThread, handleComplete := threads.NewUninterruptableThreadComplete(fmt.Sprintf("%s: External: handle", n.Name),
		n.handleMessages, &wait)

	sendThread, sendComplete := threads.NewUninterruptableThreadComplete(fmt.Sprintf("%s: External: send", n.Name),
		func(ctx context.Context) error {
			return n.sendMessages(ctx)
		}, &wait)

	handleThread.Start(ctx)
	sendThread.Start(ctx)

	var resultErr error
	select {
	case err := <-sendComplete:
		resultErr = errors.Wrap(err, "send")
	case err := <-handleComplete:
		resultErr = errors.Wrap(err, "handle")
	}

	logger.Info(ctx, "Stopping External: %s", n.Name)
	n.isClosed.Store(true)
	n.connection.Close()
	close(n.outgoingMsgChannel)
	wait.Wait()

	return resultErr
}

func (n *MockExternalNode) handleMessages(ctx context.Context) error {
	version := buildVersionMsg("", TestUserAgent, n.headers.Height(), true)
	if err := n.sendMessage(version); err != nil {
		return errors.Wrap(err, "send version")
	}

	for {
		if n.isClosed.Load().(bool) {
			return nil
		}

		if msg, _, err := wire.ReadMessage(n.connection, wire.ProtocolVersion,
			wire.BitcoinNet(TestNet)); err != nil {

			if typeError, ok := errors.Cause(err).(*wire.MessageError); ok {
				if typeError.Type == wire.MessageErrorUnknownCommand {
					continue
				}

				if typeError.Type == wire.MessageErrorConnectionClosed {
					return nil
				}
			}

			return errors.Wrap(err, "read")
		} else {
			if err := n.handleMessage(ctx, msg); err != nil {
				return errors.Wrap(err, "handle")
			}
		}
	}
}

func (n *MockExternalNode) handleMessage(ctx context.Context, msg wire.Message) error {
	switch message := msg.(type) {
	case *wire.MsgVersion:
		if err := n.sendMessage(&wire.MsgVerAck{}); err != nil {
			return errors.Wrap(err, "send ver ack")
		}

	case *wire.MsgVerAck:

	case *wire.MsgGetHeaders:
		msgHeaders := &wire.MsgHeaders{}
		foundSplit := false
		for _, hash := range message.BlockLocatorHashes {
			if hash.Equal(&headers.MainNetRequiredHeader.PrevBlock) {
				foundSplit = true
				msgHeaders.AddBlockHeader(headers.MainNetRequiredHeader)
			}
		}

		if !foundSplit {
			// Find block height and send headers.
			l := len(message.BlockLocatorHashes)
			if l == 0 {
				return errors.New("Empty block request")
			}

			lastHash := *message.BlockLocatorHashes[l-1]
			height := n.headers.HashHeight(lastHash)
			if height == -1 {
				return fmt.Errorf("Header not found: %s", lastHash)
			}

			headers, err := n.headers.GetHeaders(ctx, height, 500)
			if err != nil {
				return errors.Wrap(err, "get headers")
			}

			if len(headers) == 0 {
				return fmt.Errorf("No headers found: height %d", height)
			}

			for _, header := range headers {
				msgHeaders.AddBlockHeader(header)
			}
		}

		if err := n.sendMessage(msgHeaders); err != nil {
			return errors.Wrap(err, "send headers")
		}

	case *wire.MsgGetData:
		for _, inv := range message.InvList {
			if inv.Type != wire.InvTypeTx {
				continue
			}

			var tx *wire.MsgTx
			n.TxLock.Lock()
			for i, ltx := range n.Txs {
				if ltx.TxHash().Equal(&inv.Hash) {
					tx = ltx
					n.Txs = append(n.Txs[:i], n.Txs[i+1:]...)
					break
				}
			}
			n.TxLock.Unlock()

			if tx != nil {
				if err := n.sendMessage(tx); err != nil {
					return errors.Wrap(err, "send tx")
				}
			} else {
				logger.ErrorWithFields(ctx, []logger.Field{
					logger.Stringer("txid", inv.Hash),
				}, "Tx not found for get data request")
			}
		}
	}

	return nil
}

func (n *MockExternalNode) sendMessage(msg wire.Message) error {
	select {
	case n.outgoingMsgChannel <- msg:
		return nil
	case <-time.After(time.Second * 10):
		return errors.New("Could not add message to channel")
	}
}

func (n *MockExternalNode) sendMessages(ctx context.Context) error {
	for msg := range n.outgoingMsgChannel {
		if n.isClosed.Load().(bool) {
			return nil
		}

		if _, err := wire.WriteMessageN(n.connection, msg, wire.ProtocolVersion,
			wire.BitcoinNet(TestNet)); err != nil {
			logger.VerboseWithFields(ctx, []logger.Field{
				logger.String("command", msg.Command()),
			}, "Failed to send message : %s", err)
			for range n.outgoingMsgChannel { // flush channel
			}
			return errors.Wrap(err, "write message")
		}
	}

	return nil
}
