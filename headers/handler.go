package headers

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/tokenized/pkg/logger"
	"github.com/tokenized/pkg/wire"

	"github.com/pkg/errors"
)

func (repo *Repository) HandleHeadersMessage(ctx context.Context, header *wire.MessageHeader,
	r io.Reader) error {
	waitWarning := logger.NewWaitingWarning(ctx, 3*time.Second, "headers.HandleHeadersMessage")
	defer waitWarning.Cancel()

	count, err := wire.ReadVarInt(r, wire.ProtocolVersion)
	if err != nil {
		return errors.Wrap(err, "header count")
	}

	for i := uint64(0); i < count; i++ {
		blockHeader := &wire.BlockHeader{}
		if err := blockHeader.Deserialize(r); err != nil {
			return errors.Wrap(err, fmt.Sprintf("read header %d / %d", i, count))
		}

		txCount, err := wire.ReadVarInt(r, wire.ProtocolVersion)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("read tx count %d / %d", i, count))
		}

		if txCount != 0 {
			return fmt.Errorf("Non-zero header tx count : %d", txCount)
		}

		if err := repo.ProcessHeader(ctx, blockHeader); err != nil {
			return errors.Wrap(err, "process")
		}
	}

	return nil
}
