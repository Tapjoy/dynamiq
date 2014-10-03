package app

import "github.com/go-martini/martini"
import "log"
import "strconv"
import "fmt"
import "github.com/hashicorp/memberlist"
import "net/http"

func InitWebserver(list *memberlist.Memberlist, cfg Config) {
	//test partition
	part := InitPartitions()
	m := martini.Classic()
	m.Get("/servers", func() string {
		return_string := ""
		for _, member := range list.Members() {
			return_string += fmt.Sprintf("Member: %s %s\n", member.Name, member.Addr)
		}
		return return_string
	})
	m.Get("/fakeQueue", func() string {
		return_string := ""
		bottom, top := part.GetPartition(cfg, list)
		return_string = fmt.Sprintf("Top: %s Bottom: %s\n", strconv.Itoa(top), strconv.Itoa(bottom))
		return return_string
	})
	m.Get("/queues/:queue/message", func(params martini.Params) string {
		return "I should be a message from queue " + params["queue"]
	})
	m.Put("/queues/:queue/message", func(params martini.Params) string {
		return "I should be the message id that you just put"
	})
	m.Delete("/queues/:queue/message/:messageId", func(params martini.Params) string {
		return "I should be returning 201 no content, or throw an error"
	})
	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(cfg.Core.HttpPort), m))
}
