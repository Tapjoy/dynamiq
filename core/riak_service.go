package core

import (
	"fmt"
	"time"

	"github.com/basho/riak-go-client"
)

// RiakService is an abstraction over the client, to DRY up some common lookups
type RiakService struct {
	Pool *riak.Client
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
func (rs *RiakService) GetQueueConfigMap() (*riak.Map, error) {
	return rs.GetMap("queues_config")
}

// GetTopicConfigMap loads the primary topic configuration data from Riak, as a native riak.Map
func (rs *RiakService) GetTopicConfigMap() (*riak.Map, error) {
	return rs.GetMap("topics_config")
}

// CreateQueueConfigMap is
func (rs *RiakService) CreateQueueConfigMap() (*riak.Map, error) {
	op := &riak.MapOperation{}
	op.SetRegister("created", []byte(time.Now().String()))

	return rs.CreateOrUpdateMap("config", "queues_config", op)
}

// CreateTopicConfigMap is
func (rs *RiakService) CreateTopicConfigMap() (*riak.Map, error) {
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
		WithMapOperation(op).
		WithReturnBody(true).
		WithKey(key).Build()

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

func queueConfigRecordName(queueName string) string {
	return fmt.Sprintf("queue_%s_config", queueName)
}

func topicConfigRecordName(queueName string) string {
	return fmt.Sprintf("topic_%s_config", queueName)
}
