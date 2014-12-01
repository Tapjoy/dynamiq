package app

import (
	"bytes"
	"fmt"
	"github.com/Tapjoy/riakQueue/app/config"
	"github.com/go-martini/martini"
	"github.com/hashicorp/memberlist"
	"github.com/martini-contrib/binding"
	"github.com/martini-contrib/render"
	"log"
	"net/http"
	"strconv"
)

// TODO Should this live in the config package?
// Using pointers lets us differentiate between a natural 0, and an int-default 0
// omitempty tells it to ignore anything where the name was provided, but an empty value
// inside of a request body.
type ConfigRequest struct {
	VisibilityTimeout *int `json:"visibility_timeout,omitempty"`
	MinPartitions     *int `json:"min_partitions,omitempty"`
	MaxPartitions     *int `json:"max_partitions,omitempty"`
}

// TODO make message definitions more explicit

func InitWebserver(list *memberlist.Memberlist, cfg config.Config) {
	// tieing our Queue to HTTP interface == bad we should move this somewhere else
	queues := InitQueues(cfg.RiakPool)
	// also tieing topics this is next for refactor
	topics := InitTopics(cfg, queues)
	m := martini.Classic()
	m.Use(render.Renderer())

	// STATUS / STATISTICS API BLOCK
	m.Get("/status/servers", func() string {
		return_string := ""
		for _, member := range list.Members() {
			return_string += fmt.Sprintf("Member: %s %s\n", member.Name, member.Addr)
		}
		return return_string
	})
	// END STATUS / STATISTICS API BLOCK

	// CONFIGURATION API BLOCK
	m.Put("/topics/:topic", func(r render.Render, params martini.Params) {

	})

	m.Delete("/topics/:topic", func(r render.Render, params martini.Params) {

		var present bool
		_, present = topics.TopicMap[params["topic"]]
		if present != true {
			topics.InitTopic(params["topic"])
		}
		r.JSON(200, map[string]interface{}{"Deleted": topics.DeleteTopic(params["topic"])})
	})

	m.Put("/queues/:queue", func(r render.Render, params martini.Params) {
		cfg.InitializeQueue(params["queue"])
		r.JSON(200, "ok")
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

	m.Delete("/queues/:queue", func(r render.Render, params martini.Params) {

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

	m.Patch("/topics/:topic/queues/:queue", func(r render.Render, params martini.Params) {

	})

	m.Patch("/queues/:queue", binding.Json(ConfigRequest{}), func(configRequest ConfigRequest, r render.Render, params martini.Params) {
		// Not sure of better way to get the queue name from the request, but would be good to not
		// Have to reach into params - better to bind it

		// There is probably an optimal order to set these in, depending on if the values for min/max
		// have shrunk or grown, so the system can self-adjust in the correct fashion. Or, maybe it's fine to do it however
		var err error
		if configRequest.VisibilityTimeout != nil {
			err = cfg.SetVisibilityTimeout(params["queue"], *configRequest.VisibilityTimeout)
		}

		if configRequest.MinPartitions != nil {
			err = cfg.SetMinPartitions(params["queue"], *configRequest.MinPartitions)
		}

		if configRequest.MaxPartitions != nil {
			err = cfg.SetMaxPartitions(params["queue"], *configRequest.MaxPartitions)
		}

		if err != nil {
			// TODO This needs to be smarter, need to check err early and often
			r.JSON(500, "not ok")
		} else {
			r.JSON(200, "ok")
		}
	})

	// END CONFIGURATION API BLOCK

	// DATA INTERACTION API BLOCK

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

	m.Get("/queues/:queue", func(r render.Render, params martini.Params) {
		//check if we've initialized this queue yet
		var present bool
		_, present = queues.QueueMap[params["queue"]]
		if present != true {
			queues.InitQueue(cfg, params["queue"])
		}
		settings := cfg.GetQueueSettings(params["queue"])
		queueReturn := make(map[string]interface{})
		queueReturn["visibility"] = settings[config.VISIBILITY_TIMEOUT]
		queueReturn["min_partitions"] = settings[config.MIN_PARTITIONS]
		queueReturn["max_partitions"] = settings[config.MAX_PARTITIONS]
		queueReturn["partitions"] = queues.QueueMap[params["queue"]].Parts.PartitionCount()
		r.JSON(200, queueReturn)

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
		messageList := make([]map[string]interface{}, 0, 10)
		//Format response
		for _, object := range messages {
			message := make(map[string]interface{})
			message["id"] = object.Key
			message["body"] = string(object.Data[:])
			messageList = append(messageList, message)
		}
		if err != nil {
			log.Println(err)
			r.JSON(204, err.Error())
		} else {
			r.JSON(200, messageList)
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

	// DATA INTERACTION API BLOCK

	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(cfg.Core.HttpPort), m))
}
