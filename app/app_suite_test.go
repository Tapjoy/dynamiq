package app_test

import (
	"github.com/Tapjoy/riakQueue/app"
	"github.com/hashicorp/memberlist"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"io/ioutil"
	"log"
	"testing"
	"time"
)

var cfg app.Config
var core app.Core
var queues app.QueuesConfig
var duration time.Duration
var memberList *memberlist.Memberlist
var testQueueName = "test_queue"

func TestPartitions(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "App Suite")

}

var _ = BeforeSuite(func() {
	// Create the basic Configuration object
	// later tests can change these values as needed
	core = app.Core{
		Name:                  "john",
		Port:                  8000,
		SeedServer:            "steve",
		SeedPort:              8001,
		HttpPort:              8003,
		RiakNodes:             "127.0.0.1",
		BackendConnectionPool: 16,
		SyncConfigInterval:    duration,
	}
	queues = app.QueuesConfig{
		Settings: make(map[string]map[string]string),
	}
	queues.Settings[testQueueName] = make(map[string]string)
	queues.Settings[testQueueName][app.VISIBILITY_TIMEOUT] = "30"
	queues.Settings[testQueueName][app.MIN_PARTITIONS] = "10"
	queues.Settings[testQueueName][app.MAX_PARTITIONS] = "50"

	cfg.Core = core
	cfg.Queues = queues

	// Create a memberlist, aka the list of possible RiaQ processes to communicate with
	memberList = app.InitMember(cfg)

	// Disable log output during tests
	log.SetOutput(ioutil.Discard)
})

var _ = AfterSuite(func() {

	// Shut this down incase another suite of tests needs the port, or it's own instance
	memberList.Shutdown()
})
