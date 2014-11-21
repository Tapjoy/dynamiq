package app_test

import (
	. "github.com/Tapjoy/riakQueue/app"
	"github.com/hashicorp/memberlist"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"io/ioutil"
	"log"
	"testing"
	"time"
)

var cfg Config
var core Core
var duration time.Duration
var memberList *memberlist.Memberlist

func TestPartitions(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "App Suite")

}

var _ = BeforeSuite(func() {
	// Create the basic Configuration object
	// later tests can change these values as needed
	core = Core{
		Name:                  "john",
		Port:                  8000,
		SeedServer:            "steve",
		SeedPort:              8001,
		HttpPort:              8003,
		Visibility:            30,
		RiakNodes:             "127.0.0.1",
		BackendConnectionPool: 16,
		InitPartitions:        10,
		MaxPartitions:         50,
		SyncConfigInterval:    duration,
	}
	cfg.Core = core

	// Create a memberlist, aka the list of possible RiaQ processes to communicate with
	memberList = InitMember(cfg)

	// Disable log output during tests
	log.SetOutput(ioutil.Discard)
})

var _ = AfterSuite(func() {

	// Shut this down incase another suite of tests needs the port, or it's own instance
	memberList.Shutdown()
})
