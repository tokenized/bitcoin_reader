package bitcoin_reader

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/wire"
	"github.com/tokenized/threads"

	"github.com/pkg/errors"
)

func sizeString(size uint64) string {
	if size < 1e3 {
		return fmt.Sprintf("%d B", size)
	} else if size < 1e6 {
		return fmt.Sprintf("%0.2f KB", float64(size)/1e3)
	}

	return fmt.Sprintf("%0.2f MB", float64(size)/1e6)
}

func (n *BitcoinNode) handleMessage(ctx context.Context, connection net.Conn) error {
	n.Lock()
	network := n.config.Network
	n.Unlock()

	header, err := readHeader(connection, network)
	if err != nil {
		return errors.Wrap(err, "header")
	}

	command := header.CommandString()

	// Check for malformed commands.
	if !utf8.ValidString(command) {
		DiscardInput(connection, header.Length)
		return errors.New("Invalid command characters")
	}

	// Pass to handler based on message type if there is one for the message type.
	n.Lock()
	handler, ok := n.handlers[command]
	n.Unlock()
	if !ok || handler == nil { // No handler for this message type. Ignore it.
		if err := DiscardInput(connection, header.Length); err != nil {
			return errors.Wrap(err, "discard")
		}
		return nil
	}

	timeout := 3 * time.Second
	if command == wire.CmdTx {
		timeout = 10 * time.Second
	} else if command == wire.CmdBlock {
		timeout = time.Minute
	}

	errChan := make(chan error, 1)
	start := time.Now()
	go func() {
		errChan <- handler(ctx, header, connection)
	}()

	for {
		select {
		case err := <-errChan:
			if err != nil {
				return errors.Wrapf(err, "handle: %s", command)
			}

			return nil

		case <-time.After(timeout):
			logger.WarnWithFields(ctx, []logger.Field{
				logger.String("command", command),
				logger.String("size", sizeString(header.Length)),
				logger.MillisecondsFromNano("elapsed_ms", time.Since(start).Nanoseconds()),
			}, "Waiting for handle message")
		}
	}

	return nil
}

func (n *BitcoinNode) handleVersion(ctx context.Context, header *wire.MessageHeader,
	r io.Reader) error {

	msg := &wire.MsgVersion{}
	if err := readMessage(r, header, msg); err != nil {
		return errors.Wrap(err, "read message")
	}

	// Note: For some reason a lot of nodes have a service value of 0x24.
	// if msg.Services&ServiceFull == 0 {
	// 	return errors.Wrapf(ErrNotFullService, "0x%016x", uint64(msg.Services))
	// }

	n.handshakeChannel <- msg // trigger handshake action
	return nil
}

func (n *BitcoinNode) handleVerack(ctx context.Context, header *wire.MessageHeader,
	r io.Reader) error {

	msg := &wire.MsgVerAck{}
	if err := readMessage(r, header, msg); err != nil {
		return errors.Wrap(err, "read message")
	}

	n.handshakeChannel <- msg // trigger handshake action
	return nil
}

func (n *BitcoinNode) handleProtoconf(ctx context.Context, header *wire.MessageHeader,
	r io.Reader) error {

	counter := threads.NewWriteCounter()
	r = io.TeeReader(r, counter)
	defer DiscardInputWithCounter(r, header.Length, counter)

	msg := &wire.MsgProtoconf{}
	if err := readMessage(r, header, msg); err != nil {
		return errors.Wrap(err, "read message")
	}

	logger.VerboseWithFields(ctx, []logger.Field{
		logger.Uint64("numberOfFields", msg.NumberOfFields),
		logger.Uint32("maxReceivePayloadLength", msg.MaxReceivePayloadLength),
		logger.String("streamPolicies", msg.StreamPolicies),
	}, "Received Protoconf")

	n.Lock()
	n.protoconfCount++
	count := n.protoconfCount
	n.Unlock()

	if count > 1 {
		return errors.New("Too many protoconf messages")
	}

	return nil
}

func waitWithWarning(ctx context.Context, wait *sync.WaitGroup, name string) {
	waitWarning := logger.NewWaitingWarning(ctx, time.Second, name)
	defer waitWarning.Cancel()
	wait.Wait()
}

func deserializeBlockHeader(ctx context.Context, r io.Reader) (*wire.BlockHeader, uint64, error) {
	// defer logger.Elapsed(ctx, time.Now(), "Deserialize header")

	blockHeader := &wire.BlockHeader{}
	if err := blockHeader.Deserialize(r); err != nil {
		return nil, 0, err
	}

	txCount, err := wire.ReadVarInt(r, wire.ProtocolVersion)
	if err != nil {
		return nil, 0, err
	}

	return blockHeader, txCount, nil
}

// HandleHeadersVerify handles headers until the latest header hash is verified to ensure the node
// is on the correct chain of headers.
func (n *BitcoinNode) handleHeadersVerify(ctx context.Context, header *wire.MessageHeader,
	r io.Reader) error {

	if !n.HandshakeIsComplete() {
		logger.Info(ctx, "Discarding headers message")
		return nil
	}

	n.Lock()
	if n.headerHandler != nil {
		// Stream data through alternate header handler by teeing the reader so all read bytes will
		// go through the buffer to the other handler.
		buffer := threads.NewWaitingBuffer()
		r = io.TeeReader(r, buffer)

		var wait sync.WaitGroup
		thread := threads.NewUninterruptableThread("Handle Headers",
			func(ctx context.Context) error {
				return n.headerHandler(ctx, header, buffer)
			})
		thread.SetWait(&wait)

		thread.Start(ctx)

		// "defers" are executed LIFO, so this will happen after the discard below, which is what we
		// want so the alternate handler will see the full message.
		defer waitWithWarning(ctx, &wait, "otherHeaderHandler")
		defer buffer.Close() // we need to close the buffer to stop the thread if it didn't finish
	}
	n.Unlock()

	counter := threads.NewWriteCounter()
	rc := io.TeeReader(r, counter)
	defer DiscardInputWithCounter(r, header.Length, counter)

	count, err := wire.ReadVarInt(rc, wire.ProtocolVersion)
	if err != nil {
		return errors.Wrap(err, "header count")
	}

	if count == 0 {
		logger.Info(ctx, "Could not verify block header. Zero provided")
		n.Stop(ctx)
		return nil // remaining message data will be discarded by deferred discard above
	}

	blockHeader, txCount, err := deserializeBlockHeader(ctx, rc)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("read first header of %d", count))
	}

	if txCount != 0 {
		return fmt.Errorf("Non-zero header tx count : %d", txCount)
	}

	hash := blockHeader.BlockHash()

	if err := n.headers.VerifyHeader(ctx, blockHeader); err == nil {
		logger.VerboseWithFields(ctx, []logger.Field{
			logger.Stringer("block_hash", hash),
			logger.Uint64("header_count", count),
		}, "Chain Verified")

		if err := n.accept(ctx); err != nil {
			return err
		}
	} else {
		logger.VerboseWithFields(ctx, []logger.Field{
			logger.Stringer("block_hash", hash),
		}, "Could not verify chain: %s", err)
		n.Stop(ctx)
	}

	return nil // remaining message data will be discarded by deferred discard above
}

// HandleHeadersTrack tracks the latest header provided by the node so we know its block height.
func (n *BitcoinNode) handleHeadersTrack(ctx context.Context, header *wire.MessageHeader,
	r io.Reader) error {

	if !n.IsReady() {
		return nil
	}

	n.Lock()
	if n.headerHandler != nil {
		// Stream data through alternate header handler by teeing the reader so all read bytes will
		// go through the buffer to the other handler.
		buffer := threads.NewWaitingBuffer()
		r = io.TeeReader(r, buffer)

		var wait sync.WaitGroup
		thread := threads.NewUninterruptableThread("Handle Headers",
			func(ctx context.Context) error {
				return n.headerHandler(ctx, header, buffer)
			})
		thread.SetWait(&wait)

		thread.Start(ctx)

		// "defers" are executed LIFO, so this will happen after the discard below, which is what we
		// want so the alternate handler will see the full message.
		defer waitWithWarning(ctx, &wait, "otherHeaderHandler")
		defer buffer.Close() // we need to close the buffer to stop the thread if it didn't finish
	}
	n.Unlock()

	start := time.Now()
	counter := threads.NewWriteCounter()
	rc := io.TeeReader(r, counter)
	defer DiscardInputWithCounter(r, header.Length, counter)

	count, err := wire.ReadVarInt(rc, wire.ProtocolVersion)
	if err != nil {
		return errors.Wrap(err, "header count")
	}

	if count == 0 {
		n.Lock()
		if len(n.lastHeaderRequest) == 0 {
			logger.Verbose(ctx, "Zero headers provided. Last requested header not known")
		} else {
			hash := n.lastHeaderRequest[len(n.lastHeaderRequest)-1]
			n.lastHeaderHash = &hash
			logger.VerboseWithFields(ctx, []logger.Field{
				logger.Stringer("last_header_hash", hash),
			}, "Zero headers provided. Using last requested header")
		}
		n.Unlock()
		return nil // remaining message data will be discarded by deferred discard above
	}

	var firstHash *bitcoin.Hash32
	for i := uint64(0); i < count; i++ {
		select {
		case <-n.interrupt:
			logger.DebugWithFields(ctx, []logger.Field{
				logger.Uint64("header_index", i),
				logger.Uint64("header_count", count),
				logger.MillisecondsFromNano("elapsed_ms", time.Since(start).Nanoseconds()),
			}, "Aborting handle headers")
			return nil
		default:
		}

		blockHeader, txCount, err := deserializeBlockHeader(ctx, rc)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("read header %d/%d", i, count))
		}

		if txCount != 0 {
			return fmt.Errorf("Non-zero header tx count : %d", txCount)
		}

		hash := blockHeader.BlockHash()
		if i == 0 {
			firstHash = hash
		}

		if err := n.headers.ProcessHeader(ctx, blockHeader); err != nil {
			n.Lock()
			lastHeaderRequest := n.lastHeaderRequest
			n.Unlock()
			locatorHashes := make([]fmt.Stringer, len(lastHeaderRequest))
			for i, hash := range lastHeaderRequest {
				locatorHashes[i] = hash
			}

			logger.VerboseWithFields(ctx, []logger.Field{
				logger.Stringer("block_hash", hash),
				logger.Int("block_height", n.headers.HashHeight(*hash)),
				logger.String("issue", err.Error()),
				logger.Stringers("locator_block_hashes", locatorHashes),
			}, "Invalid block header")

			n.Stop(ctx)
			return errors.Wrap(err, "process header")
		}

		if i == count-1 {
			n.Lock()
			n.lastHeaderHash = blockHeader.BlockHash()
			n.Unlock()
		}
	}

	nanoseconds := time.Since(start).Nanoseconds()
	logger.DebugWithFields(ctx, []logger.Field{
		logger.Stringer("first_block_hash", firstHash),
		logger.Uint64("header_count", count),
		logger.MillisecondsFromNano("average_ms", nanoseconds/int64(count)),
		logger.MillisecondsFromNano("elapsed_ms", nanoseconds),
	}, "Handled headers")

	return nil
}

func (n *BitcoinNode) handlePing(ctx context.Context, header *wire.MessageHeader,
	r io.Reader) error {

	var msg wire.MsgPing
	if err := readMessage(r, header, &msg); err != nil {
		return errors.Wrap(err, "read message")
	}

	logger.Debug(ctx, "Received ping 0x%16x", msg.Nonce)

	// Send pong
	if err := n.sendMessage(ctx, &wire.MsgPong{Nonce: msg.Nonce}); err != nil {
		return errors.Wrap(err, "send pong")
	}

	return nil
}

func (n *BitcoinNode) handlePong(ctx context.Context, header *wire.MessageHeader,
	r io.Reader) error {

	var msg wire.MsgPong
	if err := readMessage(r, header, &msg); err != nil {
		return errors.Wrap(err, "read message")
	}

	n.Lock()
	sentNonce := n.pingNonce
	sent := n.pingSent
	n.Unlock()

	if sentNonce != msg.Nonce {
		return errors.New("Wrong pong nonce")
	}

	logger.Debug(ctx, "Received pong 0x%16x (%f seconds)", msg.Nonce, time.Since(sent).Seconds())

	return nil
}

func (n *BitcoinNode) handleReject(ctx context.Context, header *wire.MessageHeader,
	r io.Reader) error {

	var msg wire.MsgReject
	if err := readMessage(r, header, &msg); err != nil {
		return errors.Wrap(err, "read message")
	}

	// TODO Possibly perform actions based on reject messages. --ce
	// * Mark txs as suspected to be invalid.
	// We currently get a lot of rejects from nodes having tx fee rates set too high.
	logger.InfoWithFields(ctx, []logger.Field{
		logger.String("command", msg.Cmd),
		logger.Stringer("reject_code", msg.Code),
		logger.String("reason", msg.Reason),
		logger.Stringer("hash", msg.Hash),
	}, "Received reject")

	return nil
}

func (n *BitcoinNode) handleAddress(ctx context.Context, header *wire.MessageHeader,
	r io.Reader) error {

	msg := &wire.MsgAddr{}
	if err := readMessage(r, header, msg); err != nil {
		return errors.Wrap(err, "read message")
	}

	logger.Debug(ctx, "Received %d addresses", len(msg.AddrList))
	for _, address := range msg.AddrList {
		n.peers.Add(ctx, fmt.Sprintf("[%s]:%d", address.IP.To16().String(), address.Port))
	}

	return nil
}

func (n *BitcoinNode) handleGetAddresses(ctx context.Context, header *wire.MessageHeader,
	r io.Reader) error {

	addresses, err := buildAddressesMessage(ctx, n.peers)
	if err != nil {
		return errors.Wrap(err, "build addresses")
	}

	logger.Debug(ctx, "Sending %d addresses", len(addresses.AddrList))
	if err := n.sendMessage(ctx, addresses); err != nil {
		return errors.Wrap(err, "send addresses")
	}

	return nil
}

func (n *BitcoinNode) handleExtended(ctx context.Context, header *wire.MessageHeader,
	r io.Reader) error {

	var cmd [wire.CommandSize]byte
	if _, err := io.ReadFull(r, cmd[:]); err != nil {
		return errors.Wrap(err, "read extended command")
	}
	extendedCommand := string(bytes.TrimRight(cmd[:], string(rune(0))))

	// Read extended length into the header to be used by the handler.
	if err := binary.Read(r, endian, &header.Length); err != nil {
		return errors.Wrap(err, "read extended length")
	}

	counter := threads.NewWriteCounter()
	r = io.TeeReader(r, counter)
	defer DiscardInputWithCounter(r, header.Length, counter)

	if !n.IsReady() {
		return nil
	}

	switch extendedCommand {
	case wire.CmdBlock:
		n.Lock()
		handler, ok := n.handlers[wire.CmdBlock]
		n.Unlock()

		if ok && handler != nil {
			if err := handler(ctx, header, r); err != nil {
				return errors.Wrap(err, "handle block")
			}
		}

	case wire.CmdTx:
		n.Lock()
		handler, ok := n.handlers[wire.CmdTx]
		n.Unlock()

		if ok && handler != nil {
			if err := handler(ctx, header, r); err != nil {
				return errors.Wrap(err, "handle tx")
			}
		}

	default:
		logger.Error(ctx, "Unsupported extended message command : %s", extendedCommand)
	}

	return nil
}

func (n *BitcoinNode) handleInventory(ctx context.Context, header *wire.MessageHeader,
	r io.Reader) error {

	n.Lock()
	txManager := n.txManager
	id := n.id
	n.Unlock()

	if txManager == nil {
		return nil // remaining message data will be discarded by deferred discard above
	}

	start := time.Now()
	count, err := wire.ReadVarInt(r, wire.ProtocolVersion)
	if err != nil {
		return errors.Wrap(err, "inventory count")
	}

	// logger.InfoWithFields(ctx, []logger.Field{
	// 	logger.Uint64("count", count),
	// }, "Received inventory")

	invRequest := wire.NewMsgGetData()
	for i := uint64(0); i < count; i++ {
		select {
		case <-n.interrupt:
			logger.DebugWithFields(ctx, []logger.Field{
				logger.Uint64("inventory_index", i),
				logger.Uint64("inventory_count", count),
				logger.MillisecondsFromNano("elapsed_ms", time.Since(start).Nanoseconds()),
			}, "Aborting handle inventory")
			return nil
		default:
		}

		item, err := readInvVect(ctx, r)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("inv vect %d / %d", i, count))
		}

		if item.Type != wire.InvTypeTx {
			continue
		}

		shouldRequest, err := txManager.AddTxID(ctx, id, item.Hash)
		if err != nil {
			return errors.Wrap(err, "add txid")
		}

		if shouldRequest {
			// Request
			if err := invRequest.AddInvVect(&item); err != nil {
				// Too many requests for one message, send it and start a new message.
				if err := n.sendMessage(ctx, invRequest); err != nil {
					return errors.Wrap(err, "send tx request")
				}

				invRequest = wire.NewMsgGetData()
				if err := invRequest.AddInvVect(&item); err != nil {
					return errors.Wrap(err, "add tx to request")
				}
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

func (n *BitcoinNode) handleTx(ctx context.Context, header *wire.MessageHeader,
	r io.Reader) error {

	start := time.Now()

	n.Lock()
	txManager := n.txManager
	id := n.id
	n.Unlock()

	if txManager == nil {
		DiscardInput(r, header.Length)
		return nil
	}

	counter := threads.NewWriteCounter()
	rc := io.TeeReader(r, counter)
	rb := threads.NewReadCloser(rc)

	tx := &wire.MsgTx{}
	errChan := make(chan error, 1)
	go func() {
		errChan <- readMessage(rb, header, tx)
	}()

	timeout := time.Second * 10
	minTimeout := time.Second * time.Duration(header.Length/1e4) // Slower than 10kB per second
	if minTimeout > timeout {
		timeout = minTimeout
	}

	select {
	case err := <-errChan:
		if err != nil {
			return errors.Wrap(err, "read message")
		}

	case <-n.interrupt:
		rb.Close()
		return nil

	case <-time.After(timeout):
		rb.Close()
		remaining := header.Length - counter.Count()
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Float64("tx_size_kb", float64(header.Length)/1e3),
			logger.Float64("remaining_kb", float64(remaining)/1e3),
			logger.MillisecondsFromNano("elapsed_ms", time.Since(start).Nanoseconds()),
		}, "Transaction download too slow")
		return errors.New("Transaction download too slow")
	}

	elapsed := time.Since(start)
	if elapsed.Seconds() > 3.0 {
		logger.InfoWithFields(ctx, []logger.Field{
			logger.Stringer("txid", tx.TxHash()),
			logger.Float64("tx_size_kb", float64(header.Length)/1e3),
			logger.MillisecondsFromNano("elapsed_ms", elapsed.Nanoseconds()),
		}, "Downloaded tx")
	}

	n.Lock()
	n.txReceivedCount++
	n.txReceivedSize += counter.Count()
	n.Unlock()

	if err := txManager.AddTx(ctx, n.interrupt, id, tx); err != nil {
		return errors.Wrap(err, "add tx")
	}

	return nil
}

func discardBlock(ctx context.Context, header *wire.MessageHeader, r io.Reader,
	counter *threads.WriteCounter, blockHash *bitcoin.Hash32) {

	remaining := header.Length - counter.Count()
	if remaining > 0 {
		logger.VerboseWithFields(ctx, []logger.Field{
			logger.Stringer("block_hash", blockHash),
			logger.Float64("block_size_mb", float64(header.Length)/1e6),
			logger.Float64("remaining_mb", float64(remaining)/1e6),
		}, "Discarding remaining block message")
	}
	DiscardInputWithCounter(r, header.Length, counter)
	if remaining > 0 {
		logger.VerboseWithFields(ctx, []logger.Field{
			logger.Stringer("block_hash", blockHash),
			logger.Float64("block_size_mb", float64(header.Length)/1e6),
			logger.Float64("remaining_mb", float64(remaining)/1e6),
		}, "Finished discarding remaining block message")
	}
}

func (n *BitcoinNode) completeBlock(ctx context.Context, blockHash *bitcoin.Hash32) {
	n.Lock()
	if n.blockRequest != nil && n.blockRequest.Equal(blockHash) {
		n.requestTime = nil
		n.blockRequest = nil
		n.blockReader = nil
		delete(n.handlers, wire.CmdBlock)
		n.blockHandler = nil
		n.blockOnStop = nil
	} else {
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Stringer("current_requested_block_hash", n.blockRequest),
			logger.Stringer("receiving_block_hash", blockHash),
		}, "Different block requested before previous block completed")
	}
	n.Unlock()
}

func (n *BitcoinNode) handleBlock(ctx context.Context, header *wire.MessageHeader,
	r io.Reader) error {

	start := time.Now()
	counter := threads.NewWriteCounter()
	rc := io.TeeReader(r, counter)

	blockHeader := &wire.BlockHeader{}
	if err := blockHeader.Deserialize(rc); err != nil {
		defer discardBlock(ctx, header, r, counter, nil)
		return errors.Wrap(err, "header")
	}
	blockHash := blockHeader.BlockHash()

	defer discardBlock(ctx, header, r, counter, blockHash)

	ctx = logger.ContextWithLogFields(ctx, logger.Stringer("block_hash", blockHash))

	logger.InfoWithFields(ctx, []logger.Field{
		logger.Int("block_height", n.headers.HashHeight(*blockHash)),
		logger.Float64("block_size_mb", float64(header.Length)/1e6),
	}, "Receiving block")

	n.Lock()
	blockRequest := n.blockRequest
	blockHandler := n.blockHandler

	if blockRequest == nil {
		n.Unlock()
		logger.Warn(ctx, "No block requested")
		return nil // not a failure of the node because it could have been previously requested
	}

	if !blockRequest.Equal(blockHash) {
		n.Unlock()
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Stringer("expected_block_hash", blockRequest),
		}, "Wrong Block Received")
		return nil // not a failure of the node because it could have been previously requested
	}

	defer n.completeBlock(ctx, blockHash)

	if blockHandler == nil {
		// Block cancelled before it started downloading.
		n.Unlock()
		logger.Verbose(ctx, "Aborting block (no handler)")
		return nil
	}

	rb := threads.NewReadCloser(rc)

	n.blockReader = rb
	n.Unlock()

	// Read transaction count
	txCount, err := wire.ReadVarInt(rb, wire.ProtocolVersion)
	if err != nil {
		logger.Verbose(ctx, "Aborting block download (read tx count) : %s", err)
		return errors.Wrap(errors.Wrap(err, blockHash.String()), "read tx count")
	}

	var wait sync.WaitGroup
	txChannel := make(chan *wire.MsgTx, 1000)

	blockHandlerThread := threads.NewUninterruptableThread("Block Handler",
		func(ctx context.Context) error {
			err := blockHandler(ctx, blockHeader, txCount, txChannel)
			if err == nil {
				n.peers.UpdateScore(ctx, n.Address(), 1)
			}
			return err
		})
	blockHandlerThread.SetWait(&wait)

	blockHandlerThread.Start(ctx)

	for i := uint64(0); i < txCount; i++ {
		select {
		case <-n.interrupt:
			logger.DebugWithFields(ctx, []logger.Field{
				logger.Uint64("tx_index", i),
				logger.Uint64("tx_count", txCount),
				logger.MillisecondsFromNano("elapsed_ms", time.Since(start).Nanoseconds()),
			}, "Aborting handle block")

			close(txChannel)
			wait.Wait()
			return nil

		default:
		}

		tx := &wire.MsgTx{}
		if err := tx.Deserialize(rb); err != nil {
			close(txChannel)
			wait.Wait()
			logger.Verbose(ctx, "Aborting block download (read tx) : %s", err)
			return errors.Wrapf(errors.Wrap(err, blockHash.String()), "read tx %d", i)
		}

		txChannel <- tx
	}

	close(txChannel)
	wait.Wait()
	return nil
}
