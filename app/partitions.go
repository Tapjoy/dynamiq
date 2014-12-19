package app

import (
	"errors"
	"github.com/Tapjoy/lane"
	"github.com/hashicorp/memberlist"
	"log"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"time"
)

type Partitions struct {
	partitions *lane.PQueue
	sync.RWMutex
}

type Partition struct {
	Id       int
	LastUsed time.Time
}

func InitPartitions(cfg *Config, queueName string) *Partitions {
	part := &Partitions{
		partitions: lane.NewPQueue(lane.MINPQ),
	}
	// We'll initially allocate the minimum amount
	minPartitions, _ := cfg.GetMinPartitions(queueName)
	part.makePartitions(cfg, queueName, minPartitions)
	return part
}

func (part *Partitions) PartitionCount() int {
	return part.partitions.Size()
}
func (part *Partitions) GetPartition(cfg *Config, queueName string, list *memberlist.Memberlist) (int, int, error) {

	//get the node position and the node count
	nodePosition, nodeCount := getNodePosition(list)

	//calculate the range that our node is responsible for
	step := math.MaxInt64 / nodeCount
	nodeBottom := nodePosition * step
	nodeTop := (nodePosition + 1) * step
	myPartition, totalPartitions, err := part.getPartitionPosition(cfg, queueName)
	if err != nil {
		log.Println(err)
	}

	// calculate my range for the given number
	node_range := nodeTop - nodeBottom
	nodeStep := node_range / totalPartitions
	partitionBottom := nodeStep*myPartition + nodeBottom
	partitionTop := nodeStep*(myPartition+1) + nodeBottom
	return partitionBottom, partitionTop, err
}

//helper method to get the node position
func getNodePosition(list *memberlist.Memberlist) (int, int) {
	// figure out which node we are
	// grab and sort the node names
	nodes := list.Members()
	var nodeNames []string
	for _, node := range nodes {
		nodeNames = append(nodeNames, node.Name)
	}
	// sort our nodes so that we have a canonical ordering
	// node failure will cause more dupes
	sort.Strings(nodeNames)
	// find our index position
	nodePosition := sort.SearchStrings(nodeNames, list.LocalNode().Name)
	nodeCount := len(nodeNames)
	return nodePosition, nodeCount
}

func (part *Partitions) getPartitionPosition(cfg *Config, queueName string) (int, int, error) {
	//iterate over the partitions and then increase or decrease the number of partitions

	//TODO move loging out of the sync operation for better throughput
	myPartition := -1
	totalPartitions := part.partitions.Size()

	var err error
	poppedPartition, _ := part.partitions.Pop()
	var workingPartition *Partition
	if poppedPartition != nil {
		workingPartition = poppedPartition.(*Partition)
	} else {
		return myPartition, totalPartitions, errors.New("no available partitions")
	}
	visTimeout, _ := cfg.GetVisibilityTimeout(queueName)
	if time.Since(workingPartition.LastUsed).Seconds() > visTimeout {
		myPartition = workingPartition.Id
		workingPartition.LastUsed = time.Now()
		part.partitions.Push(workingPartition, workingPartition.LastUsed.UnixNano())
	} else {
		part.partitions.Push(workingPartition, workingPartition.LastUsed.UnixNano())
		part.Lock()
		maxPartitions, _ := cfg.GetMaxPartitions(queueName)
		if totalPartitions < maxPartitions {
			workingPartition := new(Partition)
			workingPartition.Id = totalPartitions
			workingPartition.LastUsed = time.Now()
			myPartition = workingPartition.Id
			part.partitions.Push(workingPartition, workingPartition.LastUsed.UnixNano())
		} else {
			err = errors.New("no available partitions")
		}
		part.Unlock()
	}
	return myPartition, totalPartitions, err
}

func (part *Partitions) makePartitions(cfg *Config, queueName string, partitionsToMake int) {
	var initialTime time.Time
	maxPartitions, _ := cfg.GetMaxPartitions(queueName)
	totalPartitions := part.partitions.Size()
	offset := totalPartitions
	for partitionId := offset; partitionId < offset+partitionsToMake; partitionId++ {
		if maxPartitions > partitionId {
			partition := new(Partition)
			partition.Id = partitionId
			partition.LastUsed = initialTime
			part.partitions.Push(partition, rand.Int63n(100000))
		}
	}
}
func (part *Partitions) syncPartitions(cfg *Config, queueName string) {

	maxPartitions, _ := cfg.GetMaxPartitions(queueName)
	minPartitions, _ := cfg.GetMinPartitions(queueName)
	maxPartitionAge, _ := cfg.GetMaxPartitionAge(queueName)
	totalPartitions := part.partitions.Size()
	var partsRemoved int
	for partsRemoved = 0; maxPartitions < totalPartitions; partsRemoved++ {
		_, _ = part.partitions.Pop()
		totalPartitions = totalPartitions - 1
	}

	if part.partitions.Size() < minPartitions {
		part.makePartitions(cfg, queueName, minPartitions-totalPartitions)
	}

	// Partition Aging logic
	// pop a partition
	var workingPartition *Partition
	poppedPartition, _ := part.partitions.Pop()
	if poppedPartition != nil {
		workingPartition = poppedPartition.(*Partition)
	}

	// check if the partition is older than the max age ( but not a fresh partition )
	// if true pop the next partition, continue until this condition
	for time.Since(workingPartition.LastUsed).Seconds() > maxPartitionAge && part.partitions.Size() >= minPartitions {
		poppedPartition, _ = part.partitions.Pop()
		if poppedPartition != nil {
			workingPartition = poppedPartition.(*Partition)
		}
	}
	//when false push the last popped partition
	part.partitions.Push(workingPartition, workingPartition.LastUsed.UnixNano())

}
