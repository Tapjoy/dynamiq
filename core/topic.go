package core

import (
	"sync"

	"github.com/basho/riak-go-client"
)

// Topic represents a bucket in Riak used to hold messages, and the behaviors that
// may be taken over such an object
type Topic struct {
	// the definition of a queue
	// name of the queue
	Name string
	// RiakService for interacting with Riak
	// Mutex for protecting rw access to the Config object
	configLock sync.RWMutex
	// Individual settings for the queue
	Config *riak.Map
}

// GetQueueNames is
func (t *Topic) GetQueueNames() []string {
	t.configLock.RLock()
	names := make([]string, 0)
	for _, q := range t.Config.Sets["queues"] {
		names = append(names, string(q))
	}
	t.configLock.RUnlock()
	return names
}
