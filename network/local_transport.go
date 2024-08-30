package network

import (
	"fmt"
	"sync"
)

type LocalTransport struct {
	addr      NetAddr
	consumeCh chan RPC                    // A channel used to receive messages
	lock      sync.RWMutex                // A lock to synchronize access to the peers map, ensuring thread safety.
	peers     map[NetAddr]*LocalTransport // Stores connections with other nodes
}

// Creates a new LocalTransport object and returns a pointer to it
func NewLocalTransport(addr NetAddr) Transport {
	return &LocalTransport{
		addr:      addr,
		consumeCh: make(chan RPC, 1024), // Creates a message channel with a buffer size of 1024
		peers:     make(map[NetAddr]*LocalTransport),
	}
}

// This is a method of the LocalTransport struct.
// The method returns a read-only `chan` channel, with `RPC` as the channel type.
func (t *LocalTransport) Consume() <-chan RPC {
	return t.consumeCh
}

func (t *LocalTransport) Connect(tr Transport) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	t.peers[tr.Addr()] = tr.(*LocalTransport)

	return nil
}

func (t *LocalTransport) SendMessage(to NetAddr, payload []byte) error {
	t.lock.RLock()
	defer t.lock.RUnlock()

	peer, ok := t.peers[to]
	if !ok {
		return fmt.Errorf("%s: could not send message to %s", t.addr, to)
	}

	peer.consumeCh <- RPC{
		From:    t.addr,
		Payload: payload,
	}

	return nil
}

func (t *LocalTransport) Addr() NetAddr {
	return t.addr
}
