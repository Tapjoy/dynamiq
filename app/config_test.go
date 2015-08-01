package app_test

import (
	"strconv"

	"github.com/Tapjoy/dynamiq/app"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Config", func() {

	Context("GetVisibilityTimeout", func() {
		It("should return the configured timeout for the given queue", func() {
			floatTimeout, _ := strconv.ParseFloat(app.DefaultSettings[app.VisibilityTimeout], 32)
			Expect(cfg.GetVisibilityTimeout(testQueueName)).To(Equal(floatTimeout))
		})
	})

	Context("GetMinPartitions", func() {
		It("should return the configured MinPartitions for the given queue", func() {
			intMinPartitions, _ := strconv.Atoi(app.DefaultSettings[app.MinPartitions])
			Expect(cfg.GetMinPartitions(testQueueName)).To(Equal(intMinPartitions))
		})
	})

	Context("GetMinPartitions", func() {
		It("should return the configured MaxPartitions for the given queue", func() {
			intMinPartitions, _ := strconv.Atoi(app.DefaultSettings[app.MaxPartitions])
			Expect(cfg.GetMinPartitions(testQueueName)).To(Equal(intMinPartitions))
		})
	})
})
