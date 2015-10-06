package core

// Queues represents a collection of Queues, and the behaviors that may be taken
import (
	"errors"
	"sync"
	"time"

	"github.com/basho/riak-go-client"
)

// VisibilityTimeout is the name of the config setting name for controlling how long a message is "inflight"
const VisibilityTimeout = "visibility_timeout"

// PartitionCount is
const PartitionCount = "partition_count"

// MinPartitions is the name of the config setting name for controlling the minimum number of partitions per queue
const MinPartitions = "min_partitions"

// MaxPartitions is the name of the config setting name for controlling the maximum number of partitions per node
const MaxPartitions = "max_partitions"

// MaxPartitionAge is the name of the config setting name for controlling how long an un-used partition should exist
const MaxPartitionAge = "max_partition_age"

// CompressedMessages is the name of the config setting name for controlling if the queue is using compression or not
const CompressedMessages = "compressed_messages"

var (
	// ErrQueueAlreadyExists is an error for when you try to create a queue that already exists
	ErrQueueAlreadyExists = errors.New("Queue already exists")
	// ErrConfigMapNotFound is
	ErrConfigMapNotFound = errors.New("The Config map in Riak has not yet been created")
	// Settings Arrays and maps cannot be made immutable in golang
	Settings = [...]string{VisibilityTimeout, PartitionCount, MinPartitions, MaxPartitions, CompressedMessages}

	// DefaultSettings is
	DefaultSettings = map[string]string{VisibilityTimeout: "30", PartitionCount: "5", MinPartitions: "1", MaxPartitions: "10", CompressedMessages: "false"}
)

// Queues represents a collection of Queue objects, and the behaviors that may be
// taken over a collection of such objects
type Queues struct {
	// a container for all queues
	QueueMap map[string]*Queue
	// Channels / Timer for syncing the config
	syncScheduler *time.Ticker
	syncKiller    chan bool
	// Reference to shared riak client
	riakService *RiakService
	// Mutex for protecting rw access to the Config object
	configLock sync.RWMutex
	// Settings for Queues in general, ie queue list
	Config *riak.Map
}

// LoadQueuesFromRiak is
func LoadQueuesFromRiak(cfg *Config) (*Queues, error) {
	queues := &Queues{
		QueueMap:      make(map[string]*Queue),
		syncScheduler: time.NewTicker(cfg.Riak.ConfigSyncInterval),
		syncKiller:    make(chan bool, 0),
		riakService:   cfg.Riak.Service,
		configLock:    sync.RWMutex{},
	}

	m, err := queues.riakService.GetQueueConfigMap()

	if err == ErrConfigMapNotFound {
		m, err = queues.riakService.CreateQueueConfigMap()
		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	queues.Config = m
	return queues, nil
}

// Create will register a new queue with the default config
// This queue will be available to be used once all the nodes have had their config
// refreshed
func (queues *Queues) Create(queueName string) (bool, error) {
	// This function intentionally not optimized because it should
	// not be a high-throughput operation

	if ok, err := queues.Exists(queueName); ok || err != nil {
		if err == nil {
			return false, ErrQueueAlreadyExists
		}
		return false, err
	}

	// build the operation to update the set
	op := &riak.MapOperation{}
	op.AddToSet("queues", []byte(queueName))
	_, err := queues.riakService.CreateOrUpdateMap("config", "queues_config", op)
	if err != nil {
		return false, err
	}
	return true, nil
}

// Delete will remove a queue from the system, and remove it from any topics
// that it is subscribed to. This will not delete any un-acknowledged messages
// although in the future it will expand to cover this as well.
func (queues *Queues) Delete(queueName string) (bool, error) {
	// This function intentionally not optimized because it should
	// not be a high-throughput operation
	return false, nil
}

// Exists checks is the given queue name is already created or not
func (queues *Queues) Exists(queueName string) (bool, error) {
	m, err := queues.riakService.GetQueueConfigMap()
	if err != nil {
		return false, err
	}

	if set, ok := m.Sets["queues"]; ok {
		for _, item := range set {
			if queueName == string(item) {
				return true, nil
			}
		}
	}

	return false, nil
}
