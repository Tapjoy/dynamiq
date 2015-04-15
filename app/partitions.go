package app

import (
	"errors"
	"github.com/Sirupsen/logrus"
	"github.com/Tapjoy/lane"
	"github.com/hashicorp/memberlist"
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"
)

const NOPARTITIONS string = "no available partitions"

type Partitions struct {
	partitions     *lane.PQueue
	partitionCount int
	sync.RWMutex
}

type Partition struct {
	Id       int
	LastUsed time.Time
}

func InitPartitions(cfg *Config, queueName string) *Partitions {
	part := &Partitions{
		partitions:     lane.NewPQueue(lane.MINPQ),
		partitionCount: 0,
	}
	// We'll initially allocate the minimum amount
	minPartitions, _ := cfg.GetMinPartitions(queueName)
	part.Lock()
	part.makePartitions(cfg, queueName, minPartitions)
	part.Unlock()
	return part
}

func (part *Partitions) PartitionCount() int {
	return part.partitionCount
}

func GetNodePartitionRange(cfg *Config, list *memberlist.Memberlist) (int, int) {
	//get the node position and the node count
	nodePosition, nodeCount := getNodePosition(list)

	//calculate the range that our node is responsible for
	step := math.MaxInt64 / nodeCount
	nodeBottom := nodePosition * step
	nodeTop := (nodePosition + 1) * step
	return nodeBottom, nodeTop
}

func (part *Partitions) GetPartition(cfg *Config, queueName string, list *memberlist.Memberlist) (int, int, *Partition, error) {
	//get the top and bottom for this node
	nodeBottom, nodeTop := GetNodePartitionRange(cfg, list)

	myPartition, partition, totalPartitions, err := part.getPartitionPosition(cfg, queueName)
	if err != nil && err.Error() != NOPARTITIONS {
		logrus.Error(err)
	}

	// calculate my range for the given number
	node_range := nodeTop - nodeBottom
	nodeStep := node_range / totalPartitions
	partitionBottom := nodeStep*myPartition + nodeBottom
	partitionTop := nodeStep*(myPartition+1) + nodeBottom
	return partitionBottom, partitionTop, partition, err
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

func (part *Partitions) getPartitionPosition(cfg *Config, queueName string) (int, *Partition, int, error) {
	//iterate over the partitions and then increase or decrease the number of partitions

	//TODO move loging out of the sync operation for better throughput
	myPartition := -1

	var err error
	poppedPartition, _ := part.partitions.Pop()
	var workingPartition *Partition
	if poppedPartition != nil {
		workingPartition = poppedPartition.(*Partition)
	} else {
		// this seems a little scary
		return myPartition, workingPartition, part.partitionCount, errors.New(NOPARTITIONS)
	}
	visTimeout, _ := cfg.GetVisibilityTimeout(queueName)
	if time.Since(workingPartition.LastUsed).Seconds() > visTimeout {
		myPartition = workingPartition.Id
	} else {
		part.partitions.Push(workingPartition, workingPartition.LastUsed.UnixNano())
		part.Lock()
		defer part.Unlock()
		maxPartitions, _ := cfg.GetMaxPartitions(queueName)
		if part.partitionCount < maxPartitions {
			workingPartition = new(Partition)
			workingPartition.Id = part.partitionCount
			myPartition = workingPartition.Id
			part.partitionCount = part.partitionCount + 1
		} else {
			err = errors.New(NOPARTITIONS)
		}
	}
	return myPartition, workingPartition, part.partitionCount, err
}
func (part *Partitions) PushPartition(cfg *Config, queueName string, partition *Partition, lock bool) {
	if lock {
		partition.LastUsed = time.Now()
		part.partitions.Push(partition, partition.LastUsed.UnixNano())
	} else {
		visTimeout, _ := cfg.GetVisibilityTimeout(queueName)
		unlockTime := int(visTimeout)
		partition.LastUsed = time.Now().Add(-(time.Duration(unlockTime) * time.Second))
		part.partitions.Push(partition, partition.LastUsed.UnixNano())
	}
}

func (part *Partitions) makePartitions(cfg *Config, queueName string, partitionsToMake int) {
	var initialTime time.Time
	maxPartitions, _ := cfg.GetMaxPartitions(queueName)
	offset := part.partitionCount
	for partitionId := offset; partitionId < offset+partitionsToMake; partitionId++ {
		if maxPartitions > partitionId {
			partition := new(Partition)
			partition.Id = partitionId
			partition.LastUsed = initialTime
			part.partitions.Push(partition, rand.Int63n(100000))
			part.partitionCount = part.partitionCount + 1
		}
	}
}
func (part *Partitions) syncPartitions(cfg *Config, queueName string) {

	part.Lock()
	maxPartitions, _ := cfg.GetMaxPartitions(queueName)
	minPartitions, _ := cfg.GetMinPartitions(queueName)
	maxPartitionAge, _ := cfg.GetMaxPartitionAge(queueName)

	var partsRemoved int
	for partsRemoved = 0; maxPartitions < part.partitionCount; partsRemoved++ {
		_, _ = part.partitions.Pop()
	}
	part.partitionCount = part.partitionCount - partsRemoved

	if part.partitionCount < minPartitions {
		part.makePartitions(cfg, queueName, minPartitions-part.partitionCount)
	}

	// Partition Aging logic
	// pop a partition
	var workingPartition *Partition
	poppedPartition, _ := part.partitions.Pop()
	if poppedPartition != nil {
		workingPartition = poppedPartition.(*Partition)
	} else {
		// this seems a little scary. we do a similiar thing in getPartitionPosition
		return
	}
	part.partitionCount = part.partitionCount - 1

	// check if the partition is older than the max age ( but not a fresh partition )
	// if true pop the next partition, continue until this condition
	for time.Since(workingPartition.LastUsed).Seconds() > maxPartitionAge && part.partitionCount >= minPartitions {
		poppedPartition, _ = part.partitions.Pop()
		if poppedPartition != nil {
			workingPartition = poppedPartition.(*Partition)
		}
		part.partitionCount = part.partitionCount - 1
	}
	//when false push the last popped partition
	part.partitions.Push(workingPartition, workingPartition.LastUsed.UnixNano())
	part.partitionCount = part.partitionCount + 1
	part.Unlock()
}
