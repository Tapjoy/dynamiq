package core

// Queues represents a collection of Queues, and the behaviors that may be taken
import (
	"errors"
	"log"
	"sync"

	"github.com/basho/riak-go-client"
)

// VisibilityTimeout is the name of the config setting name for controlling how long a message is "inflight"
const VisibilityTimeout = "visibility_timeout"

// PartitionCount is
const PartitionStep = "partition_step"

// CompressedMessages is the name of the config setting name for controlling if the queue is using compression or not
const CompressedMessages = "compressed_messages"

var (
	// ErrQueueAlreadyExists is an error for when you try to create a queue that already exists
	ErrQueueAlreadyExists = errors.New("Queue already exists")
	// ErrConfigMapNotFound is
	ErrConfigMapNotFound = errors.New("The Config map in Riak has not yet been created")
	//ErrNoKnownQueues
	ErrNoKnownQueues = errors.New("There are no known queues in the system")
	// Settings Arrays and maps cannot be made immutable in golang
	Settings = [...]string{VisibilityTimeout, PartitionStep, CompressedMessages}
	// DefaultSettings is
	DefaultSettings = map[string]string{VisibilityTimeout: "30", PartitionStep: "5000000", CompressedMessages: "false"}
)

// Queues represents a collection of Queue objects, and the behaviors that may be
// taken over a collection of such objects
type Queues struct {
	// Reference to shared riak client
	riakService *RiakService
	// Mutex for protecting rw access to the Config object
	configLock sync.RWMutex
	// Settings for Queues in general, ie queue list
	Config *riak.Map
	// a container for all queues
	KnownQueues map[string]*Queue
}

// LoadQueuesFromRiak is
func LoadQueuesFromRiak(cfg *RiakConfig) (*Queues, error) {
	queues := &Queues{
		KnownQueues: make(map[string]*Queue),
		riakService: cfg.Service,
		configLock:  sync.RWMutex{},
	}

	m, err := queues.riakService.GetQueuesConfigMap()

	if err == ErrConfigMapNotFound {
		m, err = queues.riakService.CreateQueuesConfigMap()
		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	queues.Config = m

	// TODO
	// Initialize a Queue object for each one in the set
	// Store it by name in KnownQueus

	return queues, nil
}

// Create will register a new queue with the default config
// This queue will be available to be used once all the nodes have had their config
// refreshed
func (queues *Queues) Create(queueName string, options map[string]string) (bool, error) {
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

	//cfgOps := make([]*riak.MapOperation, 0)
	// Create the config
	cOp := &riak.MapOperation{}
	for name, defaultValue := range DefaultSettings {
		if val, ok := options[name]; ok {
			log.Println("Found a setting for this", name, " ", val)
			cOp.SetRegister(name, []byte(val))
		} else {
			log.Println("Found a default setting for this", name, " ", val)
			cOp.SetRegister(name, []byte(defaultValue))
		}
		//cfgOps = append(cfgOps, cOp)
	}

	_, err = queues.riakService.CreateOrUpdateMap("config", queueConfigRecordName(queueName), cOp)
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

	// Also, not implemented yet!
	return false, nil
}

// Exists checks is the given queue name is already created or not
func (queues *Queues) Exists(queueName string) (bool, error) {
	m, err := queues.riakService.GetQueuesConfigMap()
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

func (q *Queues) GetMessage(queueName string, id string) (string, error) {
	return q.riakService.GetMessage(queueName, id)
}

func (q *Queues) SaveMessage(queueName string, data string) (string, error) {
	return q.riakService.StoreMessage(queueName, data)
}

// DeleteMessage is
func (queues *Queues) DeleteMessage(name string, id string) (map[string]bool, error) {
	return queues.DeleteMessages(name, []string{id})
}

// DeleteMessages is
func (queues *Queues) DeleteMessages(name string, ids []string) (map[string]bool, error) {
	return queues.riakService.DeleteMessages(name, ids)
}
