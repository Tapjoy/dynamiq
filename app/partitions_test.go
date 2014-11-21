package app_test

import (
	. "github.com/Tapjoy/riakQueue/app"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Partition", func() {
	Context("InitPartitions", func() {
		It("should return a number of partitions equal to the configured amount", func() {
			Expect(InitPartitions(cfg).PartitionCount()).To(Equal(50))
		})
	})
})
