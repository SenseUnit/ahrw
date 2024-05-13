// Package implements aggregated rendezvous hashing with O(1)
// amortized time complexity.
package ahrw

import (
	"bytes"
	"encoding/binary"
	"errors"
	"slices"
	"sync/atomic"

	"github.com/zeebo/xxh3"
)

// Server is an implementation of [Node] interface provided for convenience.
type Server struct {
	id     []byte
	handle any
}

// NewServer creates Server identified by name and holding some handle.
// Handle is useful to reference actual server object. This way Server
// wraps any type and provides NodeID inferred from unique name.
func NewServer(name string, handle any) *Server {
	return &Server{
		id:     []byte(name),
		handle: handle,
	}
}

// NodeID implements [Node] interface and returns provided unique name as
// a slice of bytes.
func (s *Server) NodeID() []byte {
	return s.id
}

// Handle returns handle passed to constructor. Useful to hold a reference
// to actual server object.
func (s *Server) Handle() any {
	return s.handle
}

// Node is the interface for load balancing targets (servers, shards etc) of
// rendezvous hashing algorithm.
type Node interface {
	// NodeID returns slice of bytes unique within set of all targets
	// provided to AHRW instance.
	NodeID() []byte
}

var _ Node = &Server{}

type noCopy struct{}

func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}

// AHRW stands for Aggregated Highest Random Weight.
//
// It implements Rendezvous Hashing, mapping objects to respective nodes
// with minimal remapping if set of nodes changes.
//
// AHRW pre-aggregates input objects into specified number of slots and
// only then distributes these slots across provided nodes. It allows
// memoization of calculations made for each slot and makes rendezvous
// hashing practical for large numbers of nodes (or shares of capacity
// for weighted case) and/or high request rates.
//
// AHRW is safe for concurrent use by multiple goroutines and for
// efficiency should only be created once and re-used. On the other hand
// AHRW instance should be recreated to change set of active nodes.
type AHRW struct {
	_     noCopy
	nodes []Node
	m     []atomic.Pointer[Node]
}

func uniqNodes(nodes []Node) []Node {
	sortedNodes := make([]Node, len(nodes))
	copy(sortedNodes, nodes)
	slices.SortStableFunc(sortedNodes, func(a, b Node) int {
		return bytes.Compare(a.NodeID(), b.NodeID())
	})
	return slices.CompactFunc(sortedNodes, func(a, b Node) bool {
		return bytes.Equal(a.NodeID(), b.NodeID())
	})
}

var (
	// ErrZeroSlots indicates incorrect invocation of New with zero slots.
	ErrZeroSlots = errors.New("number of slots can't be zero")
	// ErrZeroNodes indicates incorrect invocation of New with empty slice
	// of nodes.
	ErrZeroNodes = errors.New("number of nodes can't be zero")
	// ErrSlotOutOfRange is returned when requested slot is
	// beyond index range of created AHRW instance.
	ErrSlotOutOfRange = errors.New("slot out of range")
)

// New returns instance of AHRW with nslots slots distributed to nodes.
//
// Reasonable choice of nslots is two orders of magnitude higher than
// maximal potential number of nodes. It works a bit faster if nslots is
// power of two. E.g. for expected maximum of 100 nodes
// reasonable choice would be 16384.
//
// nslots should be the same across instances for different sets of nodes,
// otherwise AHRW will not maintain minimal difference of distribution. It's
// fine to have some hardcoded value covering needs for near future.
func New(nslots uint64, nodes []Node) (*AHRW, error) {
	if nslots == 0 {
		return nil, ErrZeroSlots
	}
	if len(nodes) == 0 {
		return nil, ErrZeroNodes
	}

	return &AHRW{
		nodes: uniqNodes(nodes),
		m:     make([]atomic.Pointer[Node], nslots),
	}, nil
}

func (h *AHRW) calculateNode(slot uint64) *Node {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, slot)

	hash := xxh3.New()

	hrw := uint64(0)
	hrwidx := 0
	for i := 0; i < len(h.nodes); i++ {
		hash.Reset()
		hash.Write(buf)
		hash.Write(h.nodes[i].NodeID())
		weight := hash.Sum64()
		if weight > hrw {
			hrw = weight
			hrwidx = i
		}
	}
	return &h.nodes[hrwidx]
}

func (h *AHRW) lookupSlot(slot uint64) Node {
	node := h.m[slot].Load()
	if node == nil {
		node = h.calculateNode(slot)
		h.m[slot].Store(node)
	}
	return *node
}

// NSlots returns number of slots in this AHRW instance.
func (h *AHRW) NSlots() uint64 {
	return uint64(len(h.m))
}

// NodeForSlot returns mapped node for specified slot.
// Useful if you'd like to implement hashing of your objects
// on your own.
func (h *AHRW) NodeForSlot(slot uint64) (Node, error) {
	if slot >= uint64(len(h.m)) {
		return nil, ErrSlotOutOfRange
	}
	return h.lookupSlot(slot), nil
}

// NodeForString maps string identifying some object to one
// of nodes provided to this AHRW instance.
func (h *AHRW) NodeForString(s string) Node {
	return h.NodeForBytes([]byte(s))
}

// NodeForBytes maps slice of bytes identifying some object to one
// of nodes provided to this AHRW instance.
func (h *AHRW) NodeForBytes(s []byte) Node {
	return h.lookupSlot(SlotForBytes(uint64(len(h.m)), s))
}

// SlotForBytes uniformly maps byte slice identifying some object
// to some slot.
func SlotForBytes(nslots uint64, s []byte) uint64 {
	if nslots == 0 {
		panic("number of slots can't be zero")
	}

	hash := xxh3.New()
	iv := []byte{0}

	if nslots&(nslots-1) == 0 {
		hash.Write(iv)
		hash.Write(s)
		return hash.Sum64() & (nslots - 1)
	}

	minBiased := -((-nslots) % nslots) // == 2**64 - (2**64%nslots)

	var hv uint64
	for {
		hash.Write(iv)
		hash.Write([]byte(s))
		hv = hash.Sum64()
		if hv < minBiased {
			break
		}
		iv[0]++
		hash.Reset()
	}
	return hv % nslots
}
