package headers

import (
	"sync"

	"github.com/tokenized/pkg/wire"

	"github.com/pkg/errors"
)

type Header struct {
	Height uint32
	Header *wire.BlockHeader
}

// HeaderChannel is used to feed new headers to the server. There is a lock around adding to the
// channel to support writers and closers in different threads.
type HeaderChannel struct {
	Channel chan Header
	lock    sync.Mutex
	open    bool
}

// NewHeaderChannel creates a new header channel.
func NewHeaderChannel() *HeaderChannel {
	return &HeaderChannel{}
}

func (c *HeaderChannel) Add(header Header) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if !c.open {
		return errors.New("Channel closed")
	}

	c.Channel <- header
	return nil
}

func (c *HeaderChannel) Open(count int) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.Channel = make(chan Header, count)
	c.open = true
	return nil
}

func (c *HeaderChannel) Close() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if !c.open {
		return errors.New("Channel closed")
	}

	close(c.Channel)
	c.open = false
	return nil
}
