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

func InitPartitions(cfg Config, queueName string) Partitions {
	part := Partitions{
		partitions: lane.NewPQueue(lane.MINPQ),
	}
	// We'll initially allocate the minimum amount
	minPartitions, _ := cfg.GetMinPartitions(queueName)
	part.makePartitions(cfg, queueName, minPartitions)
	return part
}

func (part Partitions) PartitionCount() int {
	return part.partitions.Size()
}
func (part Partitions) GetPartition(cfg Config, queueName string, list *memberlist.Memberlist) (int, int, error) {

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
	myPartition, totalPartitions, err := part.getPartitionPosition(cfg, queueName)
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

func (part Partitions) getPartitionPosition(cfg Config, queueName string) (int, int, error) {
	//iterate over the partitions and then increase or decrease the number of partitions

	//TODO move loging out of the sync operation for better throughput
	myPartition := -1
	totalPartitions := part.partitions.Size()
	log.Println("totalPartitions:" + strconv.Itoa(totalPartitions))

	var err error
	poppedPartition, _ := part.partitions.Pop()
	var workingPartition *Partition
	if poppedPartition != nil {
		workingPartition = poppedPartition.(*Partition)
	} else {
		return myPartition, totalPartitions, errors.New("no available partitions")
	}
	log.Println("partition: " + strconv.Itoa(workingPartition.Id) + " occupied time: " + strconv.FormatFloat(time.Since(workingPartition.LastUsed).Seconds(), 'f', -1, 64))
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
	log.Println("totalPartitions:" + strconv.Itoa(totalPartitions))
	return myPartition, totalPartitions, err
}

func (part Partitions) makePartitions(cfg Config, queueName string, partitionsToMake int) {
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
	log.Println("made " + strconv.Itoa(partitionsToMake))
	log.Println(strconv.Itoa(part.PartitionCount()))
}
func (part Partitions) syncPartitions(cfg Config, queueName string) {
	log.Println("syncing Partitions")

	maxPartitions, _ := cfg.GetMaxPartitions(queueName)
	minPartitions, _ := cfg.GetMinPartitions(queueName)
	maxPartitionAge, _ := cfg.GetMaxPartitionAge(queueName)
	totalPartitions := part.partitions.Size()
	var partsRemoved int
	for partsRemoved = 0; maxPartitions < totalPartitions; partsRemoved++ {
		_, _ = part.partitions.Pop()
	}
	log.Println("removed " + strconv.Itoa(partsRemoved) + " from queue " + queueName)

	if part.partitions.Size() < minPartitions {
		part.makePartitions(cfg, queueName, minPartitions-totalPartitions)
	}

	poppedPartition, _ := part.partitions.Pop()

	var workingPartition *Partition
	if poppedPartition != nil {
		workingPartition = poppedPartition.(*Partition)
	}
	if time.Since(workingPartition.LastUsed).Seconds() > maxPartitionAge {
	} else {
		part.partitions.Push(workingPartition, workingPartition.LastUsed.UnixNano())
	}
}
