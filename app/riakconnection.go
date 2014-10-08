package app

import (
	"github.com/tpjg/goriakpbc"
)

//define a pool

type RiakPool chan *riak.Client

func InitRiakPool(cfg Config) RiakPool {
	riakPool := RiakPool{make(chan *riak.Client, cfg.Core.BackendConnectionPool)}
	for i := 0; i < cfg.Core.BackendConnectionPool; i++ {
		log.Println("Initializing client pool ", i)
		client, _ := NewClient()
		client.Ping()
		PutConn(client)
	}

	return riakPool
}

//Get the a riak connection from the pool
func (riakPool RiakPool) GetConn() *riak.Client {
	conn := <-riakPool
	return conn
}

//put a riak connection back on the pool
func (riakPool RiakPool) PutConn(conn *riak.Client) {
	log.Printf("Conn backlog %v", len(riakPool))
	riakPool <- conn
}

//todo add this to the config file
func (riakPool RiakPool) NewClient() (*riak.Client, string) {
	rand.Seed(time.Now().UnixNano())
	hosts := []string{cfg.Core.RiakNodes}
	host := hosts[rand.Intn(len(hosts))]
	client := riak.NewClient(host)
	client.SetConnectTimeout(2 * time.Second)
	err := client.Connect()
	if err != nil {
		log.Println(err.Error())
		return NewClient(cfg)
	} else {
		return client, host
	}
}
