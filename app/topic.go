package app

import (
	"fmt"
	"github.com/Sirupsen/logrus"
	"github.com/tpjg/goriakpbc"
	"sync"
	"time"
)

type Topic struct {
	// store a CRDT in riak for the topic configuration including subscribers
	Name     string
	Config   *riak.RDtMap
	riakPool *riak.Client
	queues   *Queues
	// Mutex for protecting rw access to the Config object
	sync.RWMutex
}

type Topics struct {
	// global topic configuration, should contain list of all active topics
	Config *riak.RDtMap
	// topic map
	TopicMap      map[string]*Topic
	riakPool      *riak.Client
	queues        *Queues
	syncScheduler *time.Ticker
	syncKiller    chan struct{}
	// Mutex for protecting rw access to the Config object
	sync.RWMutex
}

func InitTopics(cfg *Config, queues *Queues) Topics {
	client := cfg.RiakConnection()
	bucket, err := client.NewBucketType("maps", "config")
	if err != nil {
		logrus.Error(err)
	}
	config, err := bucket.FetchMap("topicsConfig")
	if err != nil {
		logrus.Error(err)
	}
	if config.FetchSet("topics") == nil {
		topicSet := config.AddSet("topics")
		//there's a bug in the protobufs client/cant have an empty set
		topicSet.Add([]byte("default_topic"))
		err = config.Store()
	}
	if err != nil {
		logrus.Error(err)
	}
	topics := Topics{
		Config:   config,
		riakPool: cfg.RiakPool,
		queues:   queues,
		TopicMap: make(map[string]*Topic),
	}
	go topics.scheduleSync(cfg)
	return topics
}

func (topics *Topics) InitTopic(name string) {
	client := topics.riakPool
	bucket, _ := client.NewBucketType("maps", CONFIGURATION_BUCKET)
	config, _ := bucket.FetchMap(topicConfigRecordName(name))

	topic := new(Topic)
	topic.Config = config
	topic.Name = name
	topic.riakPool = topics.riakPool
	topic.queues = topics.queues
	topics.TopicMap[name] = topic

	// Add the queue to the riak store
	topics.Config.FetchSet("topics").Add([]byte(name))
	topics.Config.Store()

}

//Broadcast the message to all listening queues and return the acked writes
func (topic *Topic) Broadcast(cfg *Config, message string) map[string]string {
	queueWrites := make(map[string]string)
	// If we haven't mapped any queues to this topic yet, this will be nil
	topicQueues := topic.getConfig().FetchSet("queues")
	if topicQueues != nil {
		for _, queue := range topicQueues.GetValue() {
			//check if we've initialized this queue yet
			var present bool
			_, present = topic.queues.QueueMap[string(queue)]
			if present == true {
				uuid := topic.queues.QueueMap[string(queue)].Put(cfg, message)
				queueWrites[string(queue)] = uuid
			} else {
				// Return something indicating no queue?
				// SNS -> SQS would simply blindly accept the write and NOOP
			}
		}
	}
	return queueWrites
}

func (topic *Topic) AddQueue(cfg *Config, name string) {
	client := cfg.RiakConnection()

	bucket, err := client.NewBucketType("maps", CONFIGURATION_BUCKET)
	recordName := topicConfigRecordName(topic.Name)
	topic.Config, err = bucket.FetchMap(recordName)

	queueSet := topic.Config.AddSet("queues")
	queueSet.Add([]byte(name))
	topic.Config.Store()
	topic.Config, err = bucket.FetchMap(recordName)
	if err != nil {
		logrus.Error(err)
	}
}

func (topic *Topic) DeleteQueue(cfg *Config, name string) {
	client := cfg.RiakConnection()
	recordName := topicConfigRecordName(topic.Name)
	bucket, _ := client.NewBucketType("maps", CONFIGURATION_BUCKET)
	topic.Config, _ = bucket.FetchMap(recordName)

	topic.Config.FetchSet("queues").Remove([]byte(name))
	topic.Config.Store()
	topic.Config, _ = bucket.FetchMap(recordName)

	//TODO Need de-nitialize queue analog to initialize

}

func (topic *Topic) ListQueues() []string {
	list := make([]string, 0, 10)
	queueList := topic.Config.FetchSet("queues")
	if queueList != nil {
		for _, queueName := range queueList.GetValue() {
			list = append(list, string(queueName))
		}
	}
	return list
}

func (topics *Topics) DeleteTopic(cfg *Config, name string) bool {
	client := cfg.RiakConnection()
	bucket, err := client.NewBucketType("maps", "config")
	topicsConfig, err := bucket.FetchMap("topicsConfig")
	if err != nil {
		logrus.Error(err)
	}
	topicsConfig.FetchSet("topics").Remove([]byte(name))
	err = topicsConfig.Store()
	if err != nil {
		logrus.Error(err)
	}
	// Lock while we modify the topic name hash
	topics.Lock()
	topics.TopicMap[name].Delete(cfg)
	delete(topics.TopicMap, name)
	topics.Unlock()

	if err != nil {
		logrus.Error(err)
		return false
	} else {
		return true
	}
}

func (topic *Topic) Delete(cfg *Config) {
	client := cfg.RiakConnection()

	bucket, _ := client.NewBucketType("maps", "config")
	recordName := topicConfigRecordName(topic.Name)
	topicConfig, err := bucket.FetchMap(recordName)
	if err != nil {
		logrus.Error(err)
	}
	topicConfig.Destroy()
}

func (topics *Topics) scheduleSync(cfg *Config) {
	// If we haven't created it yet, create the ticker
	if topics.syncScheduler == nil {
		topics.syncScheduler = time.NewTicker(cfg.Core.SyncConfigInterval * time.Millisecond)
	}
	// Go routine to listen to either the scheduler or the killer
	go func(config *Config) {
		for {
			select {
			// Check to see if we have a tick
			case <-topics.syncScheduler.C:
				topics.syncConfig(cfg)
			// Check to see if we've been stopped
			case <-topics.syncKiller:
				topics.syncScheduler.Stop()
				return
			}
		}
	}(cfg)
}

//helpers
//TODO move error handling for empty config in riak to initializer
func (topics *Topics) syncConfig(cfg *Config) {
	logrus.Debug("syncing Topic config with Riak")
	//refresh the topic RDtMap
	client := cfg.RiakConnection()
	bucket, err := client.NewBucketType("maps", CONFIGURATION_BUCKET)
	if err != nil {
		// This is likely caused by a network blip against the riak node, or the node being down
		// In lieu of hard-failing the service, which can recover once riak comes back, we'll simply
		// skip this iteration of the config sync, and try again at the next interval
		logrus.Error("There was an error attempting to read the from the configuration bucket")
		logrus.Error(err)
		return
	}
	//fetch the map ignore error for event that map doesn't exist
	//TODO make these keys configurable?
	//Question is this thread safe...?
	topicsConfig, err := bucket.FetchMap("topicsConfig")
	if err != nil {
		// This is likely caused by a network blip against the riak node, or the node being down
		// In lieu of hard-failing the service, which can recover once riak comes back, we'll simply
		// skip this iteration of the config sync, and try again at the next interval
		logrus.Error("There was an error attempting to read from the queue configuration map in the configuration bucket")
		logrus.Error(err)
		return
	}
	topics.updateConfig(topicsConfig)

	//iterate the map and add or remove topics that need to be destroyed
	topicSlice := topics.Config.FetchSet("topics").GetValue()
	if topicSlice == nil {
		//bail if there aren't any topics
		return
	}
	//Is there a better way to do this?

	//iterate over the topics in riak and add the missing ones
	topicsToKeep := make(map[string]bool)
	for _, topic := range topicSlice {
		topicName := string(topic)
		var present bool
		_, present = topics.TopicMap[topicName]
		if present != true {
			topics.InitTopic(topicName)
		}
		topicsToKeep[topicName] = true

	}
	//iterate over the topics in topics.TopicMap and delete the ones no longer used
	for topic, _ := range topics.TopicMap {
		var present bool
		_, present = topicsToKeep[topic]
		if present != true {
			delete(topics.TopicMap, topic)
		}
	}

	//sync all topics with riak
	for _, topic := range topics.TopicMap {
		topic.syncConfig()
	}
}

func (topic *Topic) syncConfig() {
	//refresh the topic RDtMap
	client := topic.riakPool
	bucket, err := client.NewBucketType("maps", CONFIGURATION_BUCKET)
	if err != nil {
		logrus.Error(err)
	}
	recordName := topicConfigRecordName(topic.Name)
	rCfg, err := bucket.FetchMap(recordName)
	if err != nil {
		logrus.Error(rCfg)
		logrus.Error(err)
	}
	topic.updateConfig(rCfg)
}

func (topic *Topic) updateConfig(rCfg *riak.RDtMap) {
	topic.Lock()
	defer topic.Unlock()
	topic.Config = rCfg
}

func (topic *Topic) getConfig() *riak.RDtMap {
	topic.RLock()
	defer topic.RUnlock()
	return topic.Config
}

func (topics *Topics) updateConfig(rCfg *riak.RDtMap) {
	topics.Lock()
	defer topics.Unlock()
	topics.Config = rCfg
}

func (topics *Topics) getConfig() *riak.RDtMap {
	topics.RLock()
	defer topics.RUnlock()
	return topics.Config
}

func topicConfigRecordName(topicName string) string {
	return fmt.Sprintf("topic_%s_config", topicName)
}
