package tests

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"

	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/storage"
	"github.com/tokenized/pkg/wire"

	"github.com/pkg/errors"
)

const (
	blockTxsPath    = "block_txids"
	blockTxsVersion = uint8(0)
)

var (
	endian = binary.LittleEndian
)

type BlockTxManager struct {
	store storage.Storage
}

func NewBlockTxManager(store storage.Storage) *BlockTxManager {
	return &BlockTxManager{
		store: store,
	}
}

func (m *BlockTxManager) FetchBlockTxIDs(ctx context.Context,
	blockHash bitcoin.Hash32) ([]bitcoin.Hash32, bool, error) {

	b, err := m.store.Read(ctx, blockTxIDsPath(blockHash))
	if err != nil {
		if errors.Cause(err) == storage.ErrNotFound {
			return nil, false, nil
		}
		return nil, false, errors.Wrap(err, "read")
	}

	r := bytes.NewReader(b)

	var version uint8
	if err := binary.Read(r, endian, &version); err != nil {
		return nil, false, errors.Wrap(err, "version")
	}

	if version != 0 {
		return nil, false, errors.New("Unknown version")
	}

	count, err := wire.ReadVarInt(r, 0)
	if err != nil {
		return nil, false, errors.Wrap(err, "count")
	}

	txids := make([]bitcoin.Hash32, count)
	for i := range txids {
		if err := txids[i].Deserialize(r); err != nil {
			return nil, false, errors.Wrapf(err, "txid %d", i)
		}
	}

	return txids, true, nil
}

func (m *BlockTxManager) AppendBlockTxIDs(ctx context.Context, blockHash bitcoin.Hash32,
	txids []bitcoin.Hash32) error {

	existingTxIDs, exists, err := m.FetchBlockTxIDs(ctx, blockHash)
	if err != nil {
		return errors.Wrap(err, "fetch block txids")
	} else if !exists {
		return m.SaveBlockTxIDs(ctx, blockHash, txids)
	}

	for _, txid := range txids {
		existingTxIDs = appendHash32(existingTxIDs, txid)
	}

	return m.SaveBlockTxIDs(ctx, blockHash, existingTxIDs)
}

func appendHash32(hashes []bitcoin.Hash32, hash bitcoin.Hash32) []bitcoin.Hash32 {
	for _, h := range hashes {
		if h.Equal(&hash) {
			return hashes // already exists
		}
	}

	return append(hashes, hash)
}

func (m *BlockTxManager) SaveBlockTxIDs(ctx context.Context, blockHash bitcoin.Hash32,
	txids []bitcoin.Hash32) error {

	buf := &bytes.Buffer{}
	if err := binary.Write(buf, endian, blockTxsVersion); err != nil {
		return errors.Wrap(err, "version")
	}

	if err := wire.WriteVarInt(buf, 0, uint64(len(txids))); err != nil {
		return errors.Wrap(err, "count")
	}

	for i, txid := range txids {
		if err := txid.Serialize(buf); err != nil {
			return errors.Wrapf(err, "txid %d", i)
		}
	}

	if err := m.store.Write(ctx, blockTxIDsPath(blockHash), buf.Bytes(), nil); err != nil {
		return errors.Wrap(err, "write")
	}

	return nil
}

func blockTxIDsPath(hash bitcoin.Hash32) string {
	return fmt.Sprintf("%s/%s", blockTxsPath, hash)
}
