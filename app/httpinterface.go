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
		message_map := make(map[string]interface{})
		for _, object := range messages {
			//get the number of bytes in the data array
			message_map[object.Key] = string(object.Data[:])
		}
		if err != nil {
			log.Println(err)
			r.JSON(204, err.Error())
		} else {
			r.JSON(200, message_map)
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
	m.Delete("/queues/:queue/message/:messageId", func(params martini.Params) bool {
		var present bool
		_, present = queues.QueueMap[params["queue"]]
		if present != true {
			queues.InitQueue(cfg, params["queue"], riakPool)
		}
		return queues.QueueMap[params["queue"]].Delete(cfg, params["messageId"])
	})
	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(cfg.Core.HttpPort), m))
}
