package bitcoin_reader

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/pkg/logger"
	"github.com/tokenized/pkg/storage"

	"github.com/pkg/errors"
)

const (
	peersDefaultPath = "peers"
	peersVersion     = uint8(0)
)

type Peer struct {
	Address  string
	Score    int32
	LastTime uint32
}

type StoragePeerRepository struct {
	store     storage.Storage
	path      string
	lookup    map[string]*Peer
	list      PeerList
	lastSaved time.Time

	lock sync.Mutex
}

type PeerList []*Peer

func (l PeerList) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}

func NewPeerRepository(store storage.Storage, path string) *StoragePeerRepository {
	if len(path) == 0 {
		path = peersDefaultPath
	}

	return &StoragePeerRepository{
		store:  store,
		path:   path,
		lookup: make(map[string]*Peer),
	}
}

func (repo *StoragePeerRepository) Count() int {
	repo.lock.Lock()
	defer repo.lock.Unlock()

	return len(repo.list)
}

func (repo *StoragePeerRepository) Get(ctx context.Context,
	minScore, maxScore int32) (PeerList, error) {
	repo.lock.Lock()
	defer repo.lock.Unlock()

	result := make(PeerList, 0, 1000)
	for _, peer := range repo.list {
		if peer.Score >= minScore && (maxScore == -1 || peer.Score <= maxScore) {
			result = append(result, peer)
		}
	}

	// Shuffle the list
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(result), result.Swap)

	return result, nil
}

func (repo *StoragePeerRepository) Add(ctx context.Context, address string) (bool, error) {
	repo.lock.Lock()
	defer repo.lock.Unlock()

	_, exists := repo.lookup[address]
	if exists {
		return false, nil
	}

	// Add peer
	peer := Peer{Address: address, Score: 0}
	repo.list = append(repo.list, &peer)
	repo.lookup[peer.Address] = &peer
	return true, nil
}

func (repo *StoragePeerRepository) UpdateScore(ctx context.Context, address string,
	delta int32) bool {
	repo.lock.Lock()
	defer repo.lock.Unlock()

	peer, exists := repo.lookup[address]
	if exists {
		now := time.Now()
		peer.LastTime = uint32(now.Unix())
		peer.Score += delta
		return true
	}

	return false
}

func (repo *StoragePeerRepository) UpdateTime(ctx context.Context, address string) bool {
	repo.lock.Lock()
	defer repo.lock.Unlock()

	peer, exists := repo.lookup[address]
	if exists {
		now := time.Now()
		peer.LastTime = uint32(now.Unix())
		return true
	}

	return false
}

func (repo *StoragePeerRepository) LoadSeeds(ctx context.Context, network bitcoin.Network) {
	logger.Info(ctx, "Loading %s peer seeds", network)

	var seeds []Seed
	if network == bitcoin.MainNet {
		seeds = MainNetSeeds
	} else {
		seeds = TestNetSeeds
	}

	for _, seed := range seeds {
		ip := net.IP(seed.Bytes)
		peer := &Peer{
			Address: fmt.Sprintf("[%s]:%d", ip.To16().String(), seed.Port),
		}

		repo.list = append(repo.list, peer)
		repo.lookup[peer.Address] = peer
	}
}

// Loads peers from storage
func (repo *StoragePeerRepository) Load(ctx context.Context) error {
	repo.lock.Lock()
	defer repo.lock.Unlock()

	// Clear
	repo.list = make(PeerList, 0)
	repo.lookup = make(map[string]*Peer)

	// Get current data
	data, err := repo.store.Read(ctx, repo.path)
	if err == storage.ErrNotFound {
		return nil // Leave empty
	}
	if err != nil {
		return err
	}

	// Parse peers
	buffer := bytes.NewBuffer(data)
	var version uint8
	if err := binary.Read(buffer, binary.LittleEndian, &version); err != nil {
		return errors.Wrap(err, "Failed to read peers version")
	}

	if version != 0 {
		return errors.New("Unknown Version")
	}

	var count int32
	if err := binary.Read(buffer, binary.LittleEndian, &count); err != nil {
		return errors.Wrap(err, "Failed to read peers count")
	}

	// Reset
	repo.list = make(PeerList, 0, count)

	// Parse peers
	for {
		peer, err := readPeer(buffer, version)
		if err != nil {
			break
		}

		// Add peer
		repo.list = append(repo.list, &peer)
		repo.lookup[peer.Address] = &peer
	}

	repo.lastSaved = time.Now()

	logger.Info(ctx, "Loaded %d peers", len(repo.list))

	return nil
}

// Saves the peers to storage
func (repo *StoragePeerRepository) Save(ctx context.Context) error {
	repo.lock.Lock()
	defer repo.lock.Unlock()
	start := time.Now()

	var buffer bytes.Buffer

	// Write version
	if err := binary.Write(&buffer, binary.LittleEndian, peersVersion); err != nil {
		return err
	}

	// Write count
	if err := binary.Write(&buffer, binary.LittleEndian, int32(len(repo.list))); err != nil {
		return err
	}

	// Write peers
	for _, peer := range repo.list {
		if err := peer.write(&buffer); err != nil {
			return err
		}
	}

	if err := repo.store.Write(ctx, repo.path, buffer.Bytes(), nil); err != nil {
		return err
	}

	repo.lastSaved = time.Now()

	logger.ElapsedWithFields(ctx, start, []logger.Field{
		logger.Int("peer_count", len(repo.list)),
	}, "Saved peers")
	return nil
}

// Clears all peers from the database
func (repo *StoragePeerRepository) Clear(ctx context.Context) error {
	repo.lock.Lock()
	defer repo.lock.Unlock()

	repo.list = make(PeerList, 0)
	repo.lookup = make(map[string]*Peer)
	repo.lastSaved = time.Now()
	return repo.store.Remove(ctx, repo.path)
}

func readPeer(r io.Reader, version uint8) (Peer, error) {
	result := Peer{}

	// Read address
	var addressSize int32
	if err := binary.Read(r, binary.LittleEndian, &addressSize); err != nil {
		return result, err
	}

	addressData := make([]byte, addressSize)
	_, err := io.ReadFull(r, addressData) // Read until string terminator
	if err != nil {
		return result, err
	}
	result.Address = string(addressData)

	// Read score
	if err := binary.Read(r, binary.LittleEndian, &result.Score); err != nil {
		return result, err
	}

	// Read score
	if err := binary.Read(r, binary.LittleEndian, &result.LastTime); err != nil {
		return result, err
	}

	return result, nil
}

func (peer *Peer) write(w io.Writer) error {
	// Write address
	err := binary.Write(w, binary.LittleEndian, int32(len(peer.Address)))
	if err != nil {
		return err
	}
	_, err = w.Write([]byte(peer.Address))
	if err != nil {
		return err
	}

	// Write score
	err = binary.Write(w, binary.LittleEndian, peer.Score)
	if err != nil {
		return err
	}

	// Write time
	err = binary.Write(w, binary.LittleEndian, peer.LastTime)
	if err != nil {
		return err
	}

	return nil
}
