package bitcoin_reader

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/threads"

	"github.com/pkg/errors"
)

var (
	ErrInvalidMessage   = errors.New("Invalid Message")
	ErrConnectionClosed = errors.New("Connection Closed")
	ErrWrongNetwork     = errors.New("Wrong Network")
	ErrMessageTooLarge  = errors.New("Message Too Large")

	// The endian encoding of messages.
	endian = binary.LittleEndian
)

type MessageHandlerFunction func(context.Context, *wire.MessageHeader, io.Reader) error
type MessageHandlers map[string]MessageHandlerFunction

type MessageChannel struct {
	Channel chan wire.Message
	lock    sync.Mutex
	open    bool
}

func IsCloseError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Cause(err) == io.EOF || errors.Cause(err) == io.ErrUnexpectedEOF ||
		strings.Contains(err.Error(), "Closed") ||
		strings.Contains(err.Error(), "use of closed network connection") ||
		strings.Contains(err.Error(), "connection reset by peer")
}

// Read incoming messages.
func (n *BitcoinNode) readIncoming(ctx context.Context) error {
	for {
		n.connectionLock.Lock()
		connection := n.connection
		connectionClosedLocally := n.connectionClosedLocally
		n.connectionLock.Unlock()

		if connection == nil {
			if !connectionClosedLocally {
				logger.Info(ctx, "Connection closed remotely")
			}
			return nil // disconnected
		}

		if err := n.handleMessage(ctx, connection); err != nil {
			if IsCloseError(err) {
				n.connectionLock.Lock()
				connectionClosedLocally := n.connectionClosedLocally
				n.connectionLock.Unlock()
				if !connectionClosedLocally {
					logger.Info(ctx, "Connection closed remotely : %s", err)
				} else {
					logger.Info(ctx, "Disconnected : %s", err)
				}
				return nil
			} else {
				return errors.Wrap(err, "handle message")
			}
		}
	}
}

func (n *BitcoinNode) sendMessage(ctx context.Context, msg wire.Message) error {
	warning := logger.NewWaitingWarning(ctx, 3*time.Second, "Add outgoing message %s",
		msg.Command())
	defer warning.Cancel()
	return n.outgoingMsgChannel.Add(msg)
}

func (n *BitcoinNode) sendOutgoing(ctx context.Context) error {
	for msg := range n.outgoingMsgChannel.Channel {
		n.connectionLock.Lock()
		connection := n.connection
		n.connectionLock.Unlock()

		if connection == nil {
			for range n.outgoingMsgChannel.Channel { // flush channel
			}
			return nil // disconnected
		}

		// logger.InfoWithFields(ctx, []logger.Field{
		// 	logger.String("command", msg.Command()),
		// }, "Sending message")

		if _, err := wire.WriteMessageN(connection, msg, wire.ProtocolVersion,
			wire.BitcoinNet(n.config.Network)); err != nil {
			logger.ErrorWithFields(ctx, []logger.Field{
				logger.String("command", msg.Command()),
			}, "Failed to send message : %s", err)
			for range n.outgoingMsgChannel.Channel { // flush channel
			}
			return errors.Wrap(err, "write message")
		}
	}

	return nil
}

func (c *MessageChannel) Add(msg wire.Message) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if !c.open {
		return errors.New("Channel closed")
	}

	c.Channel <- msg
	return nil
}

func (c *MessageChannel) Open(count int) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.Channel = make(chan wire.Message, count)
	c.open = true
	return nil
}

func (c *MessageChannel) Close() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if !c.open {
		return errors.New("Channel closed")
	}

	close(c.Channel)
	c.open = false
	return nil
}

func buildVersionMsg(address, userAgent string, blockHeight int,
	receiveTxInventories bool) *wire.MsgVersion {

	// my local. This doesn't matter, we don't accept inbound connections.
	local := wire.NewNetAddressIPPort(net.IPv4(127, 0, 0, 1), 9333, 0)

	// build the address of the remote
	ip, port := parseAddress(address)
	remote := wire.NewNetAddressIPPort(ip, port, 0)

	version := wire.NewMsgVersion(remote, local, nonce(), int32(blockHeight))
	version.UserAgent = userAgent
	if receiveTxInventories {
		version.Services = ServiceFull // tells nodes to send tx inventory messages
	}

	return version
}

func parseAddress(address string) (net.IP, uint16) {
	parts := strings.Split(address, ":")
	var port uint16
	if len(parts) == 2 {
		p, err := strconv.Atoi(parts[1])
		if err == nil {
			port = uint16(p)
			address = parts[0]
		}
	}

	if strings.HasPrefix(address, "[") && strings.HasSuffix(address, "]") {
		address = address[1 : len(address)-2]
	}

	if ip := net.ParseIP(address); ip != nil {
		return ip, port
	}

	if len(address) > 2 && address[0] == '[' {
		parts := strings.Split(address[1:], "]")
		if len(parts) == 2 {
			address = parts[0] // remove port "[IP]:Port"
		}
	} else {
		parts := strings.Split(address, ":")
		if len(parts) == 2 {
			address = parts[0] // remove port "IP:Port"
			p, err := strconv.Atoi(parts[1])
			if err == nil {
				port = uint16(p)
			}
		}
	}

	if ip := net.ParseIP(address); ip != nil {
		return ip, port
	}

	return nil, 0
}

func buildAddressesMessage(ctx context.Context, peers PeerRepository) (*wire.MsgAddr, error) {
	peerList, err := peers.Get(ctx, 1, -1)
	if err != nil {
		return nil, errors.Wrap(err, "get peers")
	}

	result := wire.NewMsgAddr()
	for i, peer := range peerList {
		if i == wire.MaxAddrPerMsg {
			break
		}

		address, port := parseAddress(peer.Address)
		result.AddAddress(wire.NewNetAddressIPPort(address, port, wire.SFNodeNetwork))
	}

	return result, nil
}

func nonce() uint64 {
	buf := make([]byte, 8)
	rand.Read(buf)
	return binary.LittleEndian.Uint64(buf)
}

func (n *BitcoinNode) sendVerifyHeaderRequest(ctx context.Context) error {
	locatorHeaderHashes, err := n.headers.GetVerifyOnlyLocatorHashes(ctx)
	if err != nil {
		return errors.Wrap(err, "get verify only locator hashes")
	}

	n.Lock()
	n.lastHeaderRequest = locatorHeaderHashes
	n.Unlock()

	getheaders := wire.NewMsgGetHeaders()
	getheaders.ProtocolVersion = wire.ProtocolVersion

	for i := 0; i < len(locatorHeaderHashes); i++ {
		getheaders.AddBlockLocatorHash(&locatorHeaderHashes[i])
	}

	if err := n.sendMessage(ctx, getheaders); err != nil {
		return errors.Wrap(err, "send message")
	}

	return nil
}

func (n *BitcoinNode) sendInitialHeaderRequest(ctx context.Context) error {
	locatorHeaderHashes, err := n.headers.GetLocatorHashes(ctx, 10)
	if err != nil {
		return errors.Wrap(err, "get locator hashes")
	}

	n.Lock()
	n.lastHeaderRequest = locatorHeaderHashes
	n.Unlock()

	getheaders := wire.NewMsgGetHeaders()
	getheaders.ProtocolVersion = wire.ProtocolVersion

	for i := 0; i < len(locatorHeaderHashes); i++ {
		getheaders.AddBlockLocatorHash(&locatorHeaderHashes[i])
	}

	if err := n.sendMessage(ctx, getheaders); err != nil {
		return errors.Wrap(err, "send message")
	}

	return nil
}

func (n *BitcoinNode) sendHeaderRequest(ctx context.Context) error {
	locatorHeaderHashes, err := n.headers.GetLocatorHashes(ctx, 3)
	if err != nil {
		return errors.Wrap(err, "get locator hashes")
	}

	n.Lock()
	n.lastHeaderRequest = locatorHeaderHashes
	n.Unlock()

	getheaders := wire.NewMsgGetHeaders()
	getheaders.ProtocolVersion = wire.ProtocolVersion

	for i := 0; i < len(locatorHeaderHashes); i++ {
		getheaders.AddBlockLocatorHash(&locatorHeaderHashes[i])
	}

	if err := n.sendMessage(ctx, getheaders); err != nil {
		return errors.Wrap(err, "send message")
	}

	return nil
}

// readHeader reads a bitcoin P2P message header.
func readHeader(r io.Reader, network bitcoin.Network) (*wire.MessageHeader, error) {
	result := &wire.MessageHeader{}
	if err := binary.Read(r, endian, &result.Network); err != nil {
		return result, errors.Wrap(err, "network")
	}

	if result.Network != network {
		return result, errors.Wrap(ErrWrongNetwork, fmt.Sprintf("got %s (%08x), want %s (%08x)",
			result.Network, uint32(result.Network), network, uint32(network)))
	}

	if _, err := io.ReadFull(r, result.Command[:]); err != nil {
		return result, errors.Wrap(err, "command")
	}

	// Only read 32 bits, but convert to 64 bits in case it is updated by an extended message.
	var length32 uint32
	if err := binary.Read(r, endian, &length32); err != nil {
		return result, errors.Wrap(err, "length")
	}
	result.Length = uint64(length32)

	if _, err := io.ReadFull(r, result.Checksum[:]); err != nil {
		return result, errors.Wrap(err, "checksum")
	}

	return result, nil
}

func readMessage(r io.Reader, header *wire.MessageHeader, msg wire.Message) error {
	// Check for maximum length based on the message type as a malicious client
	// could otherwise create a well-formed header and set the length to max
	// numbers in order to exhaust the machine's memory.
	maxLength := msg.MaxPayloadLength(wire.ProtocolVersion)
	if header.Length > maxLength {
		DiscardInput(r, header.Length)
		return errors.Wrap(ErrMessageTooLarge, fmt.Sprintf("%s: %d b > %d", header.CommandString(),
			header.Length, maxLength))
	}

	var checkSum hash.Hash
	var rc io.Reader

	// Tee data read into checksum to calculate checksum. Extended messages don't use a checksum.
	if header.CommandString() != wire.CmdExtended {
		checkSum = sha256.New()
		rc = io.TeeReader(r, checkSum)
	} else {
		rc = r
	}

	// Read payload.
	payload := make([]byte, header.Length)
	if _, err := io.ReadFull(rc, payload); err != nil {
		return errors.Wrap(err, "read")
	}

	// Extended messages don't use a checksum.
	if checkSum != nil {
		// Check the checksum
		single := checkSum.Sum(nil)
		double := sha256.Sum256(single[:])
		if !bytes.Equal(double[0:4], header.Checksum[:]) {
			return errors.Wrap(ErrInvalidMessage, "bad checksum")
		}
	}

	// Unmarshal message
	if err := msg.BtcDecode(bytes.NewBuffer(payload), wire.ProtocolVersion); err != nil {
		return errors.Wrap(err, "decode")
	}

	return nil
}

func readInvVect(ctx context.Context, r io.Reader) (wire.InvVect, error) {
	var result wire.InvVect
	if err := binary.Read(r, endian, &result.Type); err != nil {
		return result, errors.Wrap(err, "read type")
	}

	if err := result.Hash.Deserialize(r); err != nil {
		return result, errors.Wrap(err, "read hash")
	}

	return result, nil
}

// DiscardInput reads and disposes of the specified number of bytes.
func DiscardInput(r io.Reader, n uint64) error {
	maxSize := uint64(1024) // 1k at a time
	numReads := n / maxSize
	bytesRemaining := n % maxSize
	if n > 0 {
		b := make([]byte, maxSize)
		for i := uint64(0); i < numReads; i++ {
			if _, err := io.ReadFull(r, b); err != nil {
				return err
			}
		}
	}
	if bytesRemaining > 0 {
		b := make([]byte, bytesRemaining)
		if _, err := io.ReadFull(r, b); err != nil {
			return err
		}
	}

	return nil
}

func DiscardInputWithCounter(r io.Reader, n uint64, counter *threads.WriteCounter) error {
	return DiscardInput(r, n-counter.Count())
}
