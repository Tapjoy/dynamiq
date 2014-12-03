package app_test

import (
	"github.com/Tapjoy/riakQueue/app"
	"github.com/hashicorp/memberlist"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/tpjg/goriakpbc"
	"github.com/tpjg/goriakpbc/pb"
	"io/ioutil"
	"log"
	"testing"
	"time"
)

var cfg app.Config
var core app.Core
var queues app.Queues
var duration time.Duration
var memberList *memberlist.Memberlist
var testQueueName = "test_queue"
var RDtMap *riak.RDtMap

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

	queueMap := make(map[string]app.Queue)
	configRDtMap := riak.RDtMap{
		Values:   make(map[riak.MapKey]interface{}),
		ToAdd:    make([]*pb.MapUpdate, 1),
		ToRemove: make([]*pb.MapField, 1),
	}

	configRDtMap.Values[riak.MapKey{Key: "max_partitions", Type: pb.MapField_REGISTER}] = &riak.RDtRegister{Value: []byte(app.DEFAULT_SETTINGS[app.MAX_PARTITIONS])}
	configRDtMap.Values[riak.MapKey{Key: "min_partitions", Type: pb.MapField_REGISTER}] = &riak.RDtRegister{Value: []byte(app.DEFAULT_SETTINGS[app.MIN_PARTITIONS])}
	configRDtMap.Values[riak.MapKey{Key: "visibility_timeout", Type: pb.MapField_REGISTER}] = &riak.RDtRegister{Value: []byte(app.DEFAULT_SETTINGS[app.VISIBILITY_TIMEOUT])}

	queue := app.Queue{
		Name:   testQueueName,
		Config: &configRDtMap,
	}
	queueMap[testQueueName] = queue

	queues = app.Queues{
		QueueMap: queueMap,
	}

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
