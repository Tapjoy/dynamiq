package app

import (
	"github.com/deckarep/golang-set"
	"github.com/tpjg/goriakpbc"
	"time"
)

type Topic struct {
	// store a CRDT in riak for the topic configuration including subscribers
	Name     string
	Config   *riak.RDtMap
	riakPool RiakPool
	queues   Queues
}

type Topics struct {
	// global topic configuration, should contain list of all active topics
	Config *riak.RDtMap
	// topic map
	TopicMap map[string]Topic
	riakPool RiakPool
	queues   Queues
}

func InitTopics(cfg Config, riakPool RiakPool, queues Queues) Topics {
	topics := Topics{
		Config:   new(riak.RDtMap),
		riakPool: riakPool,
		queues:   queues,
		TopicMap: make(map[string]Topic),
	}
	go topics.syncConfig(cfg)
	return topics
}

func (topics Topics) InitTopic(name string) {
	topics.TopicMap[name] = Topic{
		Config:   new(riak.RDtMap),
		Name:     name,
		riakPool: topics.riakPool,
		queues:   topics.queues,
	}
}

func (topics Topics) syncConfig(cfg Config) {
	for {
		//sleep for the sync interval time
		time.Sleep(cfg.Core.SyncConfigInterval * time.Millisecond)
		//refresh the topic RDtMap
		client := topics.riakPool.GetConn()
		defer topics.riakPool.PutConn(client)
		bucket, _ := client.NewBucketType("maps", "config")
		//fetch the map ignore error for event that map doesn't exist
		//TODO make these keys configurable?
		//Question is this thread safe...?
		topics.Config, _ = bucket.FetchMap("topicsConfig")
		//iterate the map and add or remove topics that need to be destroyed
		topicSlice := topics.Config.FetchSet("topics").GetValue()
		//Is there a better way to do this?

		topicSetRiak := mapset.NewSetFromSlice(topicSlice)
		topicSetSelf := mapset.NewSetFromSlice(topics.TopicMap)
		topicsToDelete := topicSetRiak.Difference(topicSetSelf)
		topicsToAdd := topicSetSelf.Difference(topicSetRiak)

		//add topics
		for topic := range topicsToAdd.Iter() {
			topics.InitTopic(topic.(string))
		}
		// remove topics
		for topic := range topicsToDelete.Iter() {
			delete(topics.TopicMap, topic.(string))
		}
	}

}
