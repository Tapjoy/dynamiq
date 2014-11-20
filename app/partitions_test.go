package app_test

import (
	. "github.com/Tapjoy/riakQueue/app"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"time"
)

var _ = Describe("Partition", func() {
	var cfg Config
	var core Core
	var duration time.Duration
	Context("InitPartitions", func() {
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

		It("should return a number of partitions equal to the configured amount", func() {
			Expect(cfg.Core.InitPartitions).To(Equal(50))
		})
	})
})
