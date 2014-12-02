package app_test

import (
	. "github.com/Tapjoy/riakQueue/app"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Partition", func() {

	var (
		partitions        Partitions
		err               error
		partitionTopId    int
		partitionBottomId int
	)

	BeforeEach(func() {
		// Load up a list of partitions
		partitions = InitPartitions(cfg, testQueueName)
	})

	Context("InitPartitions", func() {
		It("should return a number of partitions equal to the configured amount", func() {
			Expect(partitions.PartitionCount()).To(Equal(cfg.Core.InitPartitions))
		})
	})

	Context("GetPartition", func() {
		BeforeEach(func() {
			// Get the partition ids, and any errors
			partitionBottomId, partitionTopId, err = partitions.GetPartition(cfg, testQueueName, memberList)
		})

		It("should get a partitionTopId", func() {
			Expect(partitionTopId).ToNot(BeNil())
		})

		It("should get a partitionBottomId", func() {
			Expect(partitionBottomId).ToNot(BeNil())
		})

		It("should not get an error", func() {
			Expect(err).ToNot(HaveOccurred())
		})
	})
})
