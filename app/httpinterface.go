package app

import "github.com/go-martini/martini"
import "log"
import "strconv"
import "fmt"
import "github.com/hashicorp/memberlist"
import "net/http"

func Initwebserver(list *memberlist.Memberlist, cfg Config) {
	//test partition
	part := Initpartitions()
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
		top, bottom := part.Getpartition(cfg, list)
		return_string = fmt.Sprintf("Top: %s Bottom: %s\n", strconv.Itoa(top), strconv.Itoa(bottom))
		return return_string
	})
	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(cfg.Core.Httpport), m))
}
