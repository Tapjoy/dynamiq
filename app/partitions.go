package app

import (
	"errors"
	"github.com/Tapjoy/lane"
	"github.com/Tapjoy/riakQueue/app/config"
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

func InitPartitions(cfg config.Config) Partitions {
	part := Partitions{
		partitions: lane.NewPQueue(lane.MINPQ),
	}
	part.makePartitions(cfg, cfg.Core.InitPartitions)
	return part
}

func (part Partitions) PartitionCount() int {
	return part.partitions.Size()
}

func (part Partitions) GetPartition(cfg config.Config, list *memberlist.Memberlist) (int, int, error) {

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
	myPartition, totalPartitions, err := part.getPartitionPosition(cfg)
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

func (part Partitions) getPartitionPosition(cfg config.Config) (int, int, error) {
	//iterate over the partitions and then increase or decrease the number of partitions

	//TODO move loging out of the sync operation for better throughput
	myPartition := -1
	totalPartitions := part.partitions.Size()
	log.Println("totalPartitions:" + strconv.Itoa(totalPartitions))

	var err error
	poppedPartition, _ := part.partitions.Pop()
	var workingPartition *Partition
	if poppedPartition == nil {
		var lastUsed time.Time
		workingPartition = new(Partition)
		workingPartition.Id = 0
		workingPartition.LastUsed = lastUsed
	} else {
		workingPartition = poppedPartition.(*Partition)
	}
	log.Println("partition: " + strconv.Itoa(workingPartition.Id) + " occupied time: " + strconv.FormatFloat(time.Since(workingPartition.LastUsed).Seconds(), 'f', -1, 64))

	if time.Since(workingPartition.LastUsed).Seconds() > cfg.Core.Visibility {
		myPartition = workingPartition.Id
		workingPartition.LastUsed = time.Now()
		part.partitions.Push(workingPartition, workingPartition.LastUsed.UnixNano())
	} else {
		part.partitions.Push(workingPartition, workingPartition.LastUsed.UnixNano())
		part.Lock()
		if part.partitions.Size() < cfg.Core.MaxPartitions {

			workingPartition := new(Partition)
			workingPartition.Id = part.partitions.Size()
			workingPartition.LastUsed = time.Now()
			myPartition = workingPartition.Id
			part.partitions.Push(workingPartition, workingPartition.LastUsed.UnixNano())
		} else {
			err = errors.New("no available partitions")
		}
		part.Unlock()
	}
	log.Println("totalPartitions:" + strconv.Itoa(totalPartitions))
	return myPartition, part.partitions.Size(), err
}

func (part Partitions) makePartitions(cfg config.Config, partitionsToMake int) {
	part.Lock()
	defer part.Unlock()
	var initialTime time.Time
	offset := part.partitions.Size()
	for partitionId := offset; partitionId < offset+partitionsToMake; partitionId++ {
		if cfg.Core.MaxPartitions > partitionId {
			partition := new(Partition)
			partition.Id = partitionId
			partition.LastUsed = initialTime
			part.partitions.Push(partition, rand.Int63n(100000))
		}
	}
}
