package app

type topic struct {
	// store a CRDT in riak for the topic configuration including subscribers
	Name   string
	Config *riak.RDtMap
	queues *Queues
}
type topics struct {
	// global topic configuration, should contain list of all active topics
	Config *riak.RDtMap
	// topic map
	TopicMap *[string]topic
}

func InitTopics(cfg Config) {

}
