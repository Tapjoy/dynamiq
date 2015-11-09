package core

// Queues represents a collection of Queues, and the behaviors that may be taken
import (
	"errors"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/StabbyCutyou/partition_ring"
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
	DefaultSettings = map[string]string{VisibilityTimeout: "5s", PartitionStep: "922337203685477580", CompressedMessages: "false"}
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

	for _, q := range queues.Config.Sets["queues"] {
		queueName := string(q)
		rQueue, err := LoadQueueFromRiak(queues.riakService, queueName)
		if err != nil {
			return nil, err
		}
		queues.KnownQueues[queueName] = rQueue
	}
	// Need to use a general LoadFromRiak method

	return queues, nil
}

func LoadQueueFromRiak(rs *RiakService, queueName string) (*Queue, error) {
	cfg, err := rs.GetQueueConfigMap(queueName)
	if err != nil {
		return nil, err
	}

	step := cfg.Registers[PartitionStep]
	intStep, err := strconv.ParseInt(string(step), 10, 64)
	if err != nil {
		return nil, err
	}
	visTimeout := cfg.Registers[VisibilityTimeout]
	timeDuration, err := time.ParseDuration(string(visTimeout))

	if err != nil {
		return nil, err
	}

	q := &Queue{
		Name:        queueName,
		Config:      cfg,
		configLock:  sync.RWMutex{},
		riakService: rs,
		ring:        partitionring.New(0, partitionring.MaxPartitionUpperBound, intStep, timeDuration),
	}

	return q, nil
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
			cOp.SetRegister(name, []byte(val))
		} else {
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

// PollMessages does a range scan over the queue bucket and returns a map of message ids to bodies
func (queues *Queues) PollMessages(name string, batchSize uint32) (map[string]string, error) {
	queues.configLock.RLock()
	defer queues.configLock.RUnlock()
	queue, ok := queues.KnownQueues[name]
	if !ok {
		return nil, ErrUnknownQueue
	}
	lower, upper, err := queue.ring.ReserveNext()
	if err != nil {
		log.Println(err)
		return nil, err
	}
	riakObjects, err := queue.riakService.RangeScanMessages(queue.Name, batchSize, lower, upper)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	results := make(map[string]string, len(riakObjects))
	for _, obj := range riakObjects {
		results[obj.Key] = string(obj.Value)
	}
	return results, nil
}
