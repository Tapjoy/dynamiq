package app

import (
  "github.com/hashicorp/memberlist"
  "log"
  "strconv"
)

func InitMember(cfg Config) *memberlist.Memberlist {
  memberConf := memberlist.DefaultLANConfig()
  memberConf.Name = cfg.Core.Name
  memberConf.BindPort = cfg.Core.Port

  list, err := memberlist.Create(memberConf)
  if err != nil {
    log.Fatal(err)
  }
  list.Join([]string{(cfg.Core.SeedServer + ":" + strconv.Itoa(cfg.Core.SeedPort))})
  for _, member := range list.Members() {
    log.Printf("Member: %s %s\n", member.Name, member.Addr)
  }
  return list
}
