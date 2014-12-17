package app

import (
	"fmt"
	"github.com/tpjg/goriakpbc"
	"log"
	"time"
)

type Topic struct {
	// store a CRDT in riak for the topic configuration including subscribers
	Name     string
	Config   *riak.RDtMap
	riakPool RiakPool
	queues   *Queues
}

type Topics struct {
	// global topic configuration, should contain list of all active topics
	Config *riak.RDtMap
	// topic map
	TopicMap map[string]*Topic
	riakPool RiakPool
	queues   *Queues
}

func InitTopics(cfg *Config, queues *Queues) Topics {
	client := cfg.RiakPool.GetConn()
	defer cfg.RiakPool.PutConn(client)
	bucket, err := client.NewBucketType("maps", "config")
	if err != nil {
		log.Println(err)
	}
	config, err := bucket.FetchMap("topicsConfig")
	if err != nil {
		log.Println(err)
	}
	if config.FetchSet("topics") == nil {
		topicSet := config.AddSet("topics")
		//there's a bug in the protobufs client/cant have an empty set
		topicSet.Add([]byte("default_topic"))
		err = config.Store()
	}
	if err != nil {
		log.Println(err)
	}
	topics := Topics{
		Config:   config,
		riakPool: cfg.RiakPool,
		queues:   queues,
		TopicMap: make(map[string]*Topic),
	}
	go topics.syncConfig(cfg)
	return topics
}

func (topics *Topics) InitTopic(name string) {
	client := topics.riakPool.GetConn()
	defer topics.riakPool.PutConn(client)
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
	for _, queue := range topic.Config.FetchSet("queues").GetValue() {
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
	return queueWrites
}

// For the time being, this will also initialize the queue new if it does not exist
func (topic *Topic) AddQueue(cfg *Config, name string) {
	client := cfg.RiakConnection()
	defer cfg.ReleaseRiakConnection(client)

	bucket, err := client.NewBucketType("maps", CONFIGURATION_BUCKET)
	recordName := topicConfigRecordName(topic.Name)
	topic.Config, err = bucket.FetchMap(recordName)

	queueSet := topic.Config.AddSet("queues")
	queueSet.Add([]byte(name))
	topic.Config.Store()
	topic.Config, err = bucket.FetchMap(recordName)
	if err != nil {
		log.Println(err)
	}

	// Temporary hack to keep the api consistent
	// Only configure it new if it is a new queue. Otherwise, leave it alone
	if !cfg.Queues.Exists(cfg, name) {
		err = cfg.InitializeQueue(name)
		if err != nil {
			log.Println(err)
		}
	}
}

func (topic *Topic) DeleteQueue(cfg *Config, name string) {
	client := cfg.RiakConnection()
	defer cfg.ReleaseRiakConnection(client)
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

func (topics Topics) DeleteTopic(name string) bool {
	client := topics.riakPool.GetConn()
	defer topics.riakPool.PutConn(client)
	bucket, err := client.NewBucketType("maps", "config")
	topics.Config, err = bucket.FetchMap("topicsConfig")
	topics.Config.FetchSet("topics").Remove([]byte(name))
	err = topics.Config.Store()
	topics.Config, err = bucket.FetchMap("topicsConfig")
	topics.TopicMap[name].Delete()
	delete(topics.TopicMap, name)
	if err != nil {
		log.Println(err)
		return false
	} else {
		return true
	}
}

func (topic *Topic) Delete() {
	client := topic.riakPool.GetConn()
	defer topic.riakPool.PutConn(client)

	bucket, _ := client.NewBucketType("maps", "config")
	recordName := topicConfigRecordName(topic.Name)
	topic.Config, _ = bucket.FetchMap(recordName)
	topic.Config.Destroy()
}

//helpers
//TODO move error handling for empty config in riak to initializer
func (topics *Topics) syncConfig(cfg *Config) {
	for {
		log.Println("syncing Topic config with Riak")
		//refresh the topic RDtMap
		client := topics.riakPool.GetConn()
		bucket, err := client.NewBucketType("maps", CONFIGURATION_BUCKET)
		if err != nil {
			log.Println(err)
		}
		//fetch the map ignore error for event that map doesn't exist
		//TODO make these keys configurable?
		//Question is this thread safe...?
		topics.Config, err = bucket.FetchMap("topicsConfig")
		if err != nil {
			log.Println(err)
		}
		//iterate the map and add or remove topics that need to be destroyed
		topicSlice := topics.Config.FetchSet("topics").GetValue()
		if topicSlice == nil {
			//bail if there aren't any topics
			//but not before sleeping
			topics.riakPool.PutConn(client)
			time.Sleep(cfg.Core.SyncConfigInterval * time.Second)
			continue
		}
		//Is there a better way to do this?

		//iterate over the topics in riak and add the missing ones
		topicsToKeep := make(map[string]bool)
		for _, topic := range topicSlice {
			var present bool
			_, present = topics.TopicMap[string(topic)]
			if present != true {
				topics.InitTopic(string(topic))
			}
			topicsToKeep[string(topic)] = true

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
		//sleep for the configured interval
		topics.riakPool.PutConn(client)
		time.Sleep(cfg.Core.SyncConfigInterval * time.Millisecond)

	}
}

func (topic *Topic) syncConfig() {
	//refresh the topic RDtMap
	client := topic.riakPool.GetConn()
	defer topic.riakPool.PutConn(client)
	bucket, _ := client.NewBucketType("maps", CONFIGURATION_BUCKET)
	recordName := topicConfigRecordName(topic.Name)
	topic.Config, _ = bucket.FetchMap(recordName)
}

func topicConfigRecordName(topicName string) string {
	return fmt.Sprintf("topic_%s_config", topicName)
}
