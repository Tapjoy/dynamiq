package core

import (
	"math"
	"math/big"
	"sync"

	"github.com/StabbyCutyou/partition_ring"
	"github.com/tpjg/goriakpbc"
)

// Queue represents a bucket in Riak used to hold messages, and the behaviors that
// may be taken over such an object
type Queue struct {
	// the definition of a queue
	// name of the queue
	Name string
	// the PartitionRing for this queue
	ring *partitionring.PartitionRing

	// Mutex for protecting rw access to the Config object
	configLock sync.RWMutex
	// Individual settings for the queue
	Config *riak.RDtMap
}

// Define statistics keys suffixes

// QueueSentStatsSuffix is
const QueueSentStatsSuffix = "sent.count"

// QueueReceivedStatsSuffix is
const QueueReceivedStatsSuffix = "received.count"

// QueueDeletedStatsSuffix is
const QueueDeletedStatsSuffix = "deleted.count"

// QueueDepthStatsSuffix is
const QueueDepthStatsSuffix = "depth.count"

// QueueDepthAprStatsSuffix is
const QueueDepthAprStatsSuffix = "approximate_depth.count"

// QueueFillDeltaStatsSuffix
const QueueFillDeltaStatsSuffix = "fill.count"

// MaxIDSize is
var MaxIDSize = *big.NewInt(math.MaxInt64)
