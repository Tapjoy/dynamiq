package app

import (
	"github.com/tpjg/goriakpbc"
)

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
	TopicMap map[string]*topic
}

func InitTopics(cfg Config) {

}
