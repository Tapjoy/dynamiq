package app_test

import (
	"github.com/Tapjoy/dynamiq/app"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"strconv"
)

var _ = Describe("Config", func() {

	Context("GetVisibilityTimeout", func() {
		It("should return the configured timeout for the given queue", func() {
			floatTimeout, _ := strconv.ParseFloat(app.DEFAULT_SETTINGS[app.VISIBILITY_TIMEOUT], 32)
			Expect(cfg.GetVisibilityTimeout(testQueueName)).To(Equal(floatTimeout))
		})
	})

	Context("GetMinPartitions", func() {
		It("should return the configured min_partitions for the given queue", func() {
			intMinPartitions, _ := strconv.Atoi(app.DEFAULT_SETTINGS[app.MIN_PARTITIONS])
			Expect(cfg.GetMinPartitions(testQueueName)).To(Equal(intMinPartitions))
		})
	})

	Context("GetMaxPartitions", func() {
		It("should return the configured max_partitions for the given queue", func() {
			intMaxPartitions, _ := strconv.Atoi(app.DEFAULT_SETTINGS[app.MAX_PARTITIONS])
			Expect(cfg.GetMaxPartitions(testQueueName)).To(Equal(intMaxPartitions))
		})
	})
})
