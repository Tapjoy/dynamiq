package app_test

import (
	. "github.com/Tapjoy/riakQueue/app"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"testing"
	"time"
)

var cfg Config
var core Core
var duration time.Duration

func TestPartitions(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "App Suite")

}

var _ = BeforeSuite(func() {
	core = Core{
		Name:                  "john",
		Port:                  8000,
		SeedServer:            "steve",
		SeedPort:              8001,
		HttpPort:              8003,
		Visibility:            30,
		RiakNodes:             "localhost",
		BackendConnectionPool: 16,
		InitPartitions:        50,
		MaxPartitions:         50,
		SyncConfigInterval:    duration,
	}
	cfg.Core = core
})
