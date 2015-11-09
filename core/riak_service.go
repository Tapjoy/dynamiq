package core

import (
	"crypto/rand"
	"fmt"
	"math"
	"math/big"
	"sync"
	"time"

	"github.com/basho/riak-go-client"
)

// MaxMessageID is
var MaxMessageID = *big.NewInt(math.MaxInt64)

// RiakService is an abstraction over the client, to DRY up some common lookups
type RiakService struct {
	Pool *riak.Client
}

type deletedMessage struct {
	key     string
	deleted bool
}

// NewRiakService creates a RiakService to use in the app
func NewRiakService(addresses []string, port uint16) (*RiakService, error) {
	// Create a new Riak Client
	clientOpts := &riak.NewClientOptions{
		RemoteAddresses: addresses,
		Port:            port,
	}

	client, err := riak.NewClient(clientOpts)

	if err != nil {
		return nil, err
	}
	// Slap it into our struct so we can add some common lookups to it
	return &RiakService{
		Pool: client,
	}, nil
}

// Execute is lazy method to ferry to the underlying clients Execute
func (rs *RiakService) Execute(cmd riak.Command) error {
	return rs.Pool.Execute(cmd)
}

// GetQueueConfigMap loads the primary queue configuration data from Riak, as a native riak.Map
func (rs *RiakService) GetQueuesConfigMap() (*riak.Map, error) {
	return rs.GetMap("queues_config")
}

// GetTopicConfigMap loads the primary topic configuration data from Riak, as a native riak.Map
func (rs *RiakService) GetTopicsConfigMap() (*riak.Map, error) {
	return rs.GetMap("topics_config")
}

// CreateQueueConfigMap is
func (rs *RiakService) CreateQueuesConfigMap() (*riak.Map, error) {
	op := &riak.MapOperation{}
	op.SetRegister("created", []byte(time.Now().String()))

	return rs.CreateOrUpdateMap("config", "queues_config", op)
}

// CreateTopicConfigMap is
func (rs *RiakService) CreateTopicsConfigMap() (*riak.Map, error) {
	op := &riak.MapOperation{}
	op.SetRegister("created", []byte(time.Now().String()))

	return rs.CreateOrUpdateMap("config", "topics_config", op)
}

// GetMap loads a CRDT Map from Riak
func (rs *RiakService) GetMap(name string) (*riak.Map, error) {
	// TODO solution for how to centralize this knowledge
	// few things need to reach into config and they're confined to
	// private methods in 2 packages, but still. Magic strings + not DRY
	fmc, err := riak.NewFetchMapCommandBuilder().
		WithBucketType("maps").
		WithBucket("config").
		WithKey(name).Build()

	if err != nil {
		return nil, err
	}

	if err = rs.Execute(fmc); err != nil {
		return nil, err
	}
	res := fmc.(*riak.FetchMapCommand)
	if res.Error() != nil {
		return nil, res.Error()
	}
	if res.Response.IsNotFound {
		// There will be no error from res.Error() here, as it isn't an error
		return nil, ErrConfigMapNotFound
	}
	return res.Response.Map, nil
}

// CreateOrUpdateMap does exactly that because thats what the riak lib allows for
func (rs *RiakService) CreateOrUpdateMap(bucket string, key string, op *riak.MapOperation) (*riak.Map, error) {
	cmd, err := riak.NewUpdateMapCommandBuilder().
		WithBucketType("maps").
		WithBucket(bucket).
		WithReturnBody(true).
		WithKey(key).
		WithMapOperation(op).Build()

	if err != nil {
		return nil, err
	}

	if err = rs.Execute(cmd); err != nil {
		return nil, err
	}

	res := cmd.(*riak.UpdateMapCommand)
	return res.Response.Map, res.Error()
}

// CreateQueueConfig will create a configuration for a new queue
func (rs *RiakService) CreateQueueConfig(queueName string, values map[string]string) (*riak.Map, error) {
	op := &riak.MapOperation{}
	for key, value := range values {
		op.SetRegister(key, []byte(value))
	}

	return rs.CreateOrUpdateMap("config", queueConfigRecordName(queueName), op)
}

// RangeScanMessages is
func (rs *RiakService) RangeScanMessages(queueName string, numMessages uint32, lowerBound int64, upperBound int64) ([]*riak.Object, error) {
	cmd, err := riak.NewSecondaryIndexQueryCommandBuilder().
		WithBucketType("messages").
		WithBucket(queueName).
		WithIntRange(lowerBound, upperBound).
		WithMaxResults(numMessages).
		WithIndexName("id_int").Build()

	if err != nil {
		return nil, err
	}

	if err = rs.Execute(cmd); err != nil {
		return nil, err
	}

	res := cmd.(*riak.SecondaryIndexQueryCommand)
	if res.Error() != nil {
		return nil, res.Error()
	}
	return rs.lookupMessagesForRangeScanResults(queueName, res.Response.Results)
}

func (rs *RiakService) GetMessage(queueName string, messageKey string) (string, error) {
	cmd, err := riak.NewFetchValueCommandBuilder().
		WithBucketType("messages").
		WithBucket(queueName).
		WithKey(messageKey).Build()

	if err != nil {
		return "", err
	}
	if err = rs.Execute(cmd); err != nil {
		return "", err
	}

	res := cmd.(*riak.FetchValueCommand)
	if res.Error() != nil || res.Response.IsNotFound {
		return "", res.Error()
	}

	if len(res.Response.Values) > 1 {
		for _, obj := range res.Response.Values[1:len(res.Response.Values)] {
			_, err := rs.StoreMessage(queueName, string(obj.Value))
			if err != nil {
				// Couldn't save that Message
				// That would mean it's lost
				// need to incorporate a retry mechanic
			}
		}
	}

	return string(res.Response.Values[0].Value), nil
}

func (rs *RiakService) lookupMessagesForRangeScanResults(queueName string, results []*riak.SecondaryIndexQueryResult) ([]*riak.Object, error) {
	// Channel for holding the results of the io calls
	objChan := make(chan []*riak.Object, len(results))
	// Waitgroup to gate the function completing
	wg := &sync.WaitGroup{}
	// Seed it with the expected number of ops
	wg.Add(len(results))
	for _, item := range results {
		key := string(item.ObjectKey)
		// Go wide with IO requests, use a channel to communicate with a consumer, below
		go func(riakService *RiakService, w *sync.WaitGroup, c chan []*riak.Object, messageKey string) {
			defer wg.Done()
			cmd, err := riak.NewFetchValueCommandBuilder().
				WithBucketType("messages").
				WithBucket(queueName).
				WithKey(messageKey).Build()

			if err != nil {
				return // Can't do anything if there was an error. Probably good to log here?
			}

			if err = riakService.Execute(cmd); err != nil {
				return // Can't do anything?
			}

			res := cmd.(*riak.FetchValueCommand)
			if res.Error() != nil || res.Response.IsNotFound {
				return // ?
			}

			c <- res.Response.Values
			return
		}(rs, wg, objChan, key)
	}

	// Kickoff the waitgroup to close the conn once they all report in
	// Boy, I hope no weird goroutine scheduling stuff occurs or this could get...racey
	go func(waitGroup *sync.WaitGroup, oChan chan []*riak.Object) {
		waitGroup.Wait()
		close(oChan)
	}(wg, objChan)

	// Allocate it all up front to save some time
	foundMessages := make([]*riak.Object, len(results))
	// Keep track of how many actually returned, for later
	foundCount := 0
	for objs := range objChan {
		if len(objs) > 1 {
			for _, o := range objs[1:] {
				// Rewrite the sibling back into the system
				// Message siblings indicate unique id collisions, and should
				// be re-published into the system for later delivery
				_, err := rs.StoreMessage(queueName, string(o.Value))
				if err != nil {
					// Couldn't save that Message
					// That would mean it's lost
					// need to incorporate a retry mechanic
				}
			}
		}

		foundMessages[foundCount] = objs[0]
		foundCount++
	}
	// Return only the slice of messages found
	return foundMessages[:foundCount], nil
}

func (rs *RiakService) StoreMessage(queueName string, message string) (string, error) {
	randID, err := rand.Int(rand.Reader, &MaxMessageID)
	if err != nil {
		return "", err
	}

	id := randID.String()

	obj := &riak.Object{
		ContentType:     "application/json",
		Charset:         "utf-8",
		ContentEncoding: "utf-8",
		Value:           []byte(message),
	}

	obj.AddToIntIndex("id_int", int(randID.Int64()))

	cmd, err := riak.NewStoreValueCommandBuilder().
		WithBucketType("messages").
		WithBucket(queueName).
		WithKey(id).
		WithContent(obj).Build()

	if err != nil {
		return "", err
	}

	if err = rs.Execute(cmd); err != nil {
		return "", err
	}

	res := cmd.(*riak.StoreValueCommand)
	if res.Error() != nil {
		return "", err
	}

	return id, nil
}

func (rs *RiakService) DeleteMessage(queueName string, key string) (bool, error) {
	cmd, err := riak.NewDeleteValueCommandBuilder().
		WithBucketType("messages").
		WithBucket(queueName).
		WithKey(key).Build()

	if err != nil {
		return false, err
	}

	if err = rs.Execute(cmd); err != nil {
		return false, err
	}

	res := cmd.(*riak.DeleteValueCommand)
	if res.Error() != nil {
		return false, err
	}

	return res.Response, nil

}

func (rs *RiakService) DeleteMessages(queueName string, keys []string) (map[string]bool, error) {
	// Channel for holding the results of the io calls
	boolChan := make(chan *deletedMessage, len(keys))
	// Waitgroup to gate the function completing
	wg := &sync.WaitGroup{}
	// Seed it with the expected number of ops
	wg.Add(len(keys))

	results := make(map[string]bool)

	for _, mKey := range keys {
		// Kick off a go routine to delete the message
		go func(riakService *RiakService, w *sync.WaitGroup, c chan *deletedMessage, messageKey string) {
			defer w.Done()

			deleted, err := riakService.DeleteMessage(queueName, messageKey)
			if err == nil {
				// Pop the results onto the channel
				c <- &deletedMessage{key: messageKey, deleted: deleted}
			}
			return
		}(rs, wg, boolChan, mKey)
	}

	// Kickoff the waitgroup to close the conn once they all report in
	// Boy, I hope no weird goroutine scheduling stuff occurs or this could get...racey
	go func(waitGroup *sync.WaitGroup, c chan *deletedMessage) {
		waitGroup.Wait()
		close(c)
	}(wg, boolChan)

	// Harvest until the channel closes
	for obj := range boolChan {
		results[obj.key] = obj.deleted
	}
	return results, nil
}

func (rs *RiakService) UpdateTopicSubscription(topicName string, queueName string, addQueue bool) (bool, error) {
	// Probably less awkward to just have the calling code build the cmd but...
	// I'm trying to keep all the riak stuff in one place

	// Also - this needs to verify both exist before attempting to map them
	// It doesn't, currently
	op := &riak.MapOperation{}
	if addQueue {
		op.AddToSet("queues", []byte(queueName))
	} else {
		op.RemoveFromSet("queues", []byte(queueName))
	}
	cmd, err := riak.NewUpdateMapCommandBuilder().
		WithBucketType("maps").
		WithBucket("config").
		WithKey(topicConfigRecordName(topicName)).
		WithMapOperation(op).Build()

	if err != nil {
		return false, err
	}

	if err = rs.Execute(cmd); err != nil {
		return false, err
	}
	res := cmd.(*riak.UpdateMapCommand)
	if res.Error() != nil {
		return false, res.Error()
	}

	// If the op is a success, its a success
	return res.Success(), nil
}

func (rs *RiakService) GetQueueConfigMap(queueName string) (*riak.Map, error) {
	return rs.GetMap(queueConfigRecordName(queueName))
}

func (rs *RiakService) GetTopicConfigMap(topicName string) (*riak.Map, error) {
	return rs.GetMap(topicConfigRecordName(topicName))
}

func queueConfigRecordName(queueName string) string {
	return fmt.Sprintf("queue_%s_config", queueName)
}

func topicConfigRecordName(topicName string) string {
	return fmt.Sprintf("topic_%s_config", topicName)
}
