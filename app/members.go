package app

import (
	"github.com/Sirupsen/logrus"
	"github.com/hashicorp/memberlist"
	"strconv"
)

func InitMember(cfg *Config) *memberlist.Memberlist {
	memberConf := memberlist.DefaultLANConfig()
	memberConf.Name = cfg.Core.Name
	memberConf.BindPort = cfg.Core.Port

	list, err := memberlist.Create(memberConf)
	if err != nil {
		logrus.Fatal(err)
	}
	list.Join([]string{(cfg.Core.SeedServer + ":" + strconv.Itoa(cfg.Core.SeedPort))})
	for _, member := range list.Members() {
		logrus.Printf("Member: %s %s\n", member.Name, member.Addr)
	}
	return list
}
