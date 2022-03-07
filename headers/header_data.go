package headers

import (
	"io"
	"math/big"

	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/wire"

	"github.com/pkg/errors"
)

const (
	headerDataSerializeSize = 112 // bytes for each header data
)

type HeaderData struct {
	Hash            bitcoin.Hash32
	Header          *wire.BlockHeader
	AccumulatedWork *big.Int
}

func (h HeaderData) Serialize(w io.Writer) error {
	if err := h.Header.Serialize(w); err != nil {
		return errors.Wrap(err, "header")
	}

	if err := serializeBigInt(h.AccumulatedWork, w); err != nil {
		return errors.Wrap(err, "work")
	}

	return nil
}

func (h *HeaderData) Deserialize(r io.Reader) error {
	h.Header = &wire.BlockHeader{}
	if err := h.Header.Deserialize(r); err != nil {
		return errors.Wrap(err, "header")
	}
	h.Hash = *h.Header.BlockHash()

	work, err := deserializeBigInt(r)
	if err != nil {
		return errors.Wrap(err, "work")
	}
	h.AccumulatedWork = work

	return nil
}

func serializeBigInt(value *big.Int, w io.Writer) error {
	b := value.Bytes()
	full := make([]byte, 32)
	copy(full[32-len(b):], b)

	if _, err := w.Write(full); err != nil {
		return errors.Wrap(err, "bytes")
	}

	return nil
}

func deserializeBigInt(r io.Reader) (*big.Int, error) {
	b := make([]byte, 32)
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, errors.Wrap(err, "bytes")
	}

	result := &big.Int{}
	result.SetBytes(b)

	return result, nil
}
