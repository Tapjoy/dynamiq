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
func (part *Partitions) GetPartition(cfg *Config, queueName string, list *memberlist.Memberlist) (int, int, *Partition, error) {

	//get the node position and the node count
	nodePosition, nodeCount := getNodePosition(list)
	log.Println("Node Position: " + strconv.Itoa(nodePosition))
	log.Println("Node count: " + strconv.Itoa(nodeCount))

	//calculate the range that our node is responsible for
	step := math.MaxInt64 / nodeCount
	nodeBottom := nodePosition * step
	nodeTop := (nodePosition + 1) * step
	log.Println("Node Bottom: " + strconv.Itoa(nodeBottom))
	log.Println("Node Top: " + strconv.Itoa(nodeTop))
	myPartition, partition, totalPartitions, err := part.getPartitionPosition(cfg, queueName)
	if err != nil {
		log.Println(err)
	}

	// calculate my range for the given number
	node_range := nodeTop - nodeBottom
	log.Println("Node Range: " + strconv.Itoa(node_range))
	nodeStep := node_range / totalPartitions
	log.Println("nodeStep: " + strconv.Itoa(nodeStep))
	partitionBottom := nodeStep*myPartition + nodeBottom
	log.Println("my partition: " + strconv.Itoa(myPartition))
	log.Println("partitionBottom: " + strconv.Itoa(partitionBottom))
	partitionTop := nodeStep*(myPartition+1) + nodeBottom
	log.Println("partitionTop: " + strconv.Itoa(partitionTop))
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
	log.Println("totalPartitions:" + strconv.Itoa(part.partitionCount))

	var err error
	poppedPartition, _ := part.partitions.Pop()
	var workingPartition *Partition
	if poppedPartition != nil {
		workingPartition = poppedPartition.(*Partition)
	} else {
		// this seems a little scary
		return myPartition, workingPartition, part.partitionCount, errors.New("no available partitions")
	}
	log.Println("partition: " + strconv.Itoa(workingPartition.Id) + " occupied time: " + strconv.FormatFloat(time.Since(workingPartition.LastUsed).Seconds(), 'f', -1, 64))
	visTimeout, _ := cfg.GetVisibilityTimeout(queueName)
	if time.Since(workingPartition.LastUsed).Seconds() > visTimeout {
		myPartition = workingPartition.Id
	} else {
		part.partitions.Push(workingPartition, workingPartition.LastUsed.UnixNano())
		part.Lock()
		maxPartitions, _ := cfg.GetMaxPartitions(queueName)
		if part.partitionCount < maxPartitions {
			workingPartition := new(Partition)
			workingPartition.Id = part.partitionCount
			myPartition = workingPartition.Id
			part.partitionCount = part.partitionCount + 1
		} else {
			err = errors.New("no available partitions")
		}
		part.Unlock()
	}
	log.Println("totalPartitions:" + strconv.Itoa(part.partitionCount))
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
	log.Println("removed " + strconv.Itoa(partsRemoved) + "partitions from queue " + queueName)

	if part.partitionCount < minPartitions {
		part.makePartitions(cfg, queueName, minPartitions-part.partitionCount)
	}

	// Partition Aging logic
	// pop a partition
	var workingPartition *Partition
	poppedPartition, _ := part.partitions.Pop()
	if poppedPartition != nil {
		workingPartition = poppedPartition.(*Partition)
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
	log.Println(strconv.Itoa(part.partitionCount))
	log.Println(strconv.Itoa(part.partitions.Size()))
}
