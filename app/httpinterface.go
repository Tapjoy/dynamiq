package app

import (
	"fmt"
	"github.com/go-martini/martini"
	"github.com/hashicorp/memberlist"
	"github.com/martini-contrib/render"
	//"github.com/tpjg/goriakpbc"
	"log"
	"net/http"
	"strconv"
)

func InitWebserver(list *memberlist.Memberlist, cfg Config) {
	// tieing our Queue to HTTP interface == bad we should move this somewhere else
	queues := InitQueues()
	//test partition
	m := martini.Classic()
	m.Use(render.Renderer())
	m.Get("/status/servers", func() string {
		return_string := ""
		for _, member := range list.Members() {
			return_string += fmt.Sprintf("Member: %s %s\n", member.Name, member.Addr)
		}
		return return_string
	})
	m.Get("/queues/:queue/messages/:batchSize", func(r render.Render, params martini.Params) {
		//check if we've initialized this queue yet
		var present bool
		_, present = queues.QueueMap[params["queue"]]
		if present != true {
			queues.InitQueue(params["queue"])
		}
		batchSize, err := strconv.ParseUint(params["batchSize"], 10, 32)
		if err != nil {
			//log the error for unparsable input
			log.Println(err)
			r.JSON(422, err.Error())
		}
		messages := queues.QueueMap[params["queue"]].Get(cfg, list, uint32(batchSize))
		//TODO move this into the Queue.Get code
		message_map := make(map[string]interface{})
		for _, object := range messages {
			//get the number of bytes in the data array
			message_map[object.Key] = string(object.Data[:])
		}
		r.JSON(200, message_map)

	})
	m.Put("/queues/:queue/messages", func(params martini.Params, req *http.Request) string {
		var present bool
		_, present = queues.QueueMap[params["queue"]]
		if present != true {
			queues.InitQueue(params["queue"])
		}
		uuid := queues.QueueMap[params["queue"]].Put("test")

		return uuid
	})
	m.Delete("/queues/:queue/message/:messageId", func(params martini.Params) string {
		return "I should be returning 201 no content, or throw an error"
	})
	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(cfg.Core.HttpPort), m))
}
