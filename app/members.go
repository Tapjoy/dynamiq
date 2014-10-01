package app

import "log"
import "github.com/hashicorp/memberlist"
import "strconv"

func Initmember(cfg Config) *memberlist.Memberlist {
	member_conf := memberlist.DefaultLANConfig()
	member_conf.Name = cfg.Core.Name
	member_conf.BindPort = cfg.Core.Port

	list, err := memberlist.Create(member_conf)
	if err != nil {
		log.Fatal(err)
	}
	list.Join([]string{(cfg.Core.Seedserver + ":" + strconv.Itoa(cfg.Core.Seedport))})
	for _, member := range list.Members() {
		log.Printf("Member: %s %s\n", member.Name, member.Addr)
	}
	return list
}
