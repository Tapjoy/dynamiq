package core

import (
	"sync"

	"github.com/StabbyCutyou/partition_ring"
	"github.com/basho/riak-go-client"
)

// Queue represents a bucket in Riak used to hold messages, and the behaviors that
// may be taken over such an object
type Queue struct {
	// the definition of a queue
	// name of the queue
	Name string
	// the PartitionRing for this queue
	ring *partitionring.PartitionRing
	// the RiakService
	riakService *RiakService
	// Mutex for protecting rw access to the Config object
	configLock sync.RWMutex
	// Individual settings for the queue
	Config *riak.Map
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

// PollMessages does a range scan over the queue bucket and returns a map of message ids to bodies
func (queue *Queue) PollMessages(batchSize uint32) (map[string]string, error) {
	lower, upper, err := queue.ring.ReserveNext()
	if err != nil {
		return nil, err
	}
	riakObjects, err := queue.riakService.RangeScanMessages(queue.Name, batchSize, lower, upper)
	if err != nil {
		return nil, err
	}
	results := make(map[string]string, len(riakObjects))
	for _, obj := range riakObjects {
		results[obj.Key] = string(obj.Value)
	}
	return results, err
}

// DeleteMessage is
func (queue *Queue) DeleteMessage(id string) (map[string]bool, error) {
	return queue.DeleteMessages([]string{id})
}

// DeleteMessages is
func (queue *Queue) DeleteMessages(ids []string) (map[string]bool, error) {
	return queue.riakService.DeleteMessages(queue.Name, ids)
}
