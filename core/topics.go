package core

import (
	"errors"
	"log"
	"sync"
	"time"

	"github.com/basho/riak-go-client"
)

// Topics is
type Topics struct {
	riakService *RiakService

	syncScheduler *time.Ticker
	syncKiller    chan bool

	configLock  sync.RWMutex
	Config      *riak.Map
	KnownTopics map[string]*Topic
}

var (
	//ErrNoKnownTopics is
	ErrNoKnownTopics = errors.New("There are no known topics in the system")
	// ErrTopicAlreadyExists is
	ErrTopicAlreadyExists = errors.New("Topic already exists")
)

// LoadTopicsFromRiak is
func LoadTopicsFromRiak(cfg *RiakConfig) (*Topics, error) {
	topics := &Topics{
		KnownTopics:   make(map[string]*Topic),
		syncScheduler: time.NewTicker(cfg.ConfigSyncInterval),
		syncKiller:    make(chan bool, 0),
		riakService:   cfg.Service,
		configLock:    sync.RWMutex{},
	}

	m, err := topics.riakService.GetTopicsConfigMap()
	if err == ErrConfigMapNotFound {
		m, err = topics.riakService.CreateTopicsConfigMap()
		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	topics.Config = m
	return topics, nil
}

// Create will register a new topic
// This queue will be available to be used once all the nodes have had their config
// refreshed
func (topics *Topics) Create(queueName string) (bool, error) {
	// This function intentionally not optimized because it should
	// not be a high-throughput operation

	if ok, err := topics.Exists(queueName); ok || err != nil {
		if err == nil {
			return false, ErrTopicAlreadyExists
		}
		return false, err
	}
	// build the operation to update the set
	op := &riak.MapOperation{}
	op.AddToSet("topics", []byte(queueName))
	_, err := topics.riakService.CreateOrUpdateMap("config", "topics_config", op)
	if err != nil {
		log.Println(err)
		return false, err
	}
	return true, nil
}

// Delete will remove a topic from the system.
// this will not delete any queues or messages.
func (topics *Topics) Delete(queueName string) (bool, error) {
	// This function intentionally not optimized because it should
	// not be a high-throughput operation
	return false, nil
}

// Exists checks is the given topic name is already created or not
func (topics *Topics) Exists(topicName string) (bool, error) {
	m, err := topics.riakService.GetTopicsConfigMap()
	if err != nil {
		return false, err
	}
	if set, ok := m.Sets["topics"]; ok {
		for _, item := range set {
			if topicName == string(item) {
				return true, nil
			}
		}
	}
	return false, nil
}

func (topics *Topics) SubscribeQueue(topicName string, queueName string) (bool, error) {
	// Add the queue to the topic
	return topics.riakService.UpdateTopicSubscription(topicName, queueName, true)
}

func (topics *Topics) UnsubscribeQueue(topicName string, queueName string) (bool, error) {
	// Remove the queue from the topic
	return topics.riakService.UpdateTopicSubscription(topicName, queueName, false)
}

func (t *Topics) BroadcastMessage(topicName string, data string) ([]map[string]interface{}, error) {
	t.configLock.RLock()
	topic, ok := t.KnownTopics[topicName]
	if !ok {
		return nil, ErrUnknownTopic
	}
	results := make([]map[string]interface{}, 0)
	for _, q := range topic.Config.Sets["queues"] {
		queueName := string(q)
		log.Println("Queue is", queueName)
		id, err := t.riakService.StoreMessage(queueName, data)
		result := map[string]interface{}{"id": id, "queue": queueName}
		if err != nil {
			result[queueName] = err.Error()
		}
		results = append(results, result)
	}
	t.configLock.RUnlock()
	return results, nil
}
