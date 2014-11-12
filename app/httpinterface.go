package app

import (
	"bytes"
	"fmt"
	"github.com/go-martini/martini"
	"github.com/hashicorp/memberlist"
	"github.com/martini-contrib/render"
	"log"
	"net/http"
	"strconv"
)

//TODO make message definitions more explicit

func InitWebserver(list *memberlist.Memberlist, cfg Config) {
	//init the connectionPool
	riakPool := InitRiakPool(cfg)
	// tieing our Queue to HTTP interface == bad we should move this somewhere else
	queues := InitQueues(riakPool)
	// also tieing topics this is next for refactor
	topics := InitTopics(cfg, riakPool, queues)

	m := martini.Classic()
	m.Use(render.Renderer())
	m.Get("/status/servers", func() string {
		return_string := ""
		for _, member := range list.Members() {
			return_string += fmt.Sprintf("Member: %s %s\n", member.Name, member.Addr)
		}
		return return_string
	})
	m.Get("/status/:queue/partitions", func(r render.Render, params martini.Params) {

		//check if we've initialized this queue yet
		var present bool
		_, present = queues.QueueMap[params["queue"]]
		if present != true {
			queues.InitQueue(cfg, params["queue"])
		}
		r.JSON(200, map[string]interface{}{"paritions": queues.QueueMap[params["queue"]].Parts.PartitionCount()})
	})
	m.Get("/topics", func(r render.Render) {
		topicList := make([]string, 0, 10)
		for topicName, _ := range topics.TopicMap {
			topicList = append(topicList, topicName)
		}
		r.JSON(200, map[string]interface{}{"topics": topicList})
	})
	m.Get("/topics/:topic", func(r render.Render, params martini.Params) {
		var present bool
		_, present = topics.TopicMap[params["topic"]]
		if present != true {
			topics.InitTopic(params["topic"])
		}

		r.JSON(200, map[string]interface{}{"Queues": topics.TopicMap[params["topic"]].ListQueues()})
	})

	m.Put("/topics/:topic/queues/:queue", func(r render.Render, params martini.Params) {
		var present bool
		_, present = topics.TopicMap[params["topic"]]
		if present != true {
			topics.InitTopic(params["topic"])
		}
		topics.TopicMap[params["topic"]].AddQueue(params["queue"])
		r.JSON(200, map[string]interface{}{"Queues": topics.TopicMap[params["topic"]].ListQueues()})
	})

	// neeeds a little work....
	m.Delete("/topics/:topic/queues/:queue", func(r render.Render, params martini.Params) {
		var present bool
		_, present = topics.TopicMap[params["topic"]]
		if present != true {
			topics.InitTopic(params["topic"])
		}
		topics.TopicMap[params["topic"]].DeleteQueue(params["queue"])
		r.JSON(200, map[string]interface{}{"Queues": topics.TopicMap[params["topic"]].ListQueues()})
	})

	m.Delete("/topics/:topic", func(r render.Render, params martini.Params) {

		var present bool
		_, present = topics.TopicMap[params["topic"]]
		if present != true {
			topics.InitTopic(params["topic"])
		}
		r.JSON(200, map[string]interface{}{"Deleted": topics.DeleteTopic(params["topic"])})
	})

	m.Put("/topics/:topic/message", func(r render.Render, params martini.Params, req *http.Request) {

		var present bool
		_, present = topics.TopicMap[params["topic"]]
		if present != true {
			topics.InitTopic(params["topic"])
		}
		var buf bytes.Buffer
		buf.ReadFrom(req.Body)

		response := topics.TopicMap[params["topic"]].Broadcast(cfg, buf.String())
		r.JSON(200, response)
	})

	m.Get("/queues/:queue/messages/:batchSize", func(r render.Render, params martini.Params) {
		//check if we've initialized this queue yet
		var present bool
		_, present = queues.QueueMap[params["queue"]]
		if present != true {
			queues.InitQueue(cfg, params["queue"])
		}
		batchSize, err := strconv.ParseUint(params["batchSize"], 10, 32)
		if err != nil {
			//log the error for unparsable input
			log.Println(err)
			r.JSON(422, err.Error())
		}
		messages, err := queues.QueueMap[params["queue"]].Get(cfg, list, uint32(batchSize))
		//TODO move this into the Queue.Get code
		messageReturn := make(map[string][]map[string]interface{})
		messageList := make([]map[string]interface{}, 0, 10)
		for _, object := range messages {
			//messageReturn[object.Key] = string(object.Data[:])
			message := make(map[string]interface{})
			message["id"] = object.Key
			message["body"] = string(object.Data[:])
			messageList = append(messageList, message)
		}
		messageReturn["messages"] = messageList
		if err != nil {
			log.Println(err)
			r.JSON(204, err.Error())
		} else {
			r.JSON(200, messageReturn)
		}
	})
	m.Put("/queues/:queue/messages", func(params martini.Params, req *http.Request) string {
		var present bool
		_, present = queues.QueueMap[params["queue"]]
		if present != true {
			queues.InitQueue(cfg, params["queue"])
		}

		// parse the request body into a sting
		// TODO clean this up, full json api?
		var buf bytes.Buffer
		buf.ReadFrom(req.Body)
		uuid := queues.QueueMap[params["queue"]].Put(cfg, buf.String())

		return uuid
	})
	m.Delete("/queues/:queue/message/:messageId", func(r render.Render, params martini.Params) {
		var present bool
		_, present = queues.QueueMap[params["queue"]]
		if present != true {
			queues.InitQueue(cfg, params["queue"])
		}

		r.JSON(200, queues.QueueMap[params["queue"]].Delete(cfg, params["messageId"]))
	})
	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(cfg.Core.HttpPort), m))
}
