package app_test

import (
  . "github.com/onsi/ginkgo"
  . "github.com/onsi/gomega"
)

var _ = Describe("Partition", func() {
  Context("InitPartitions", func() {
    It("should return a number of partitions equal to the configured amount", func() {
      Expect(cfg.Core.InitPartitions).To(Equal(50))
    })
  })
})
