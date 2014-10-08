package app

import (
	"errors"
	"github.com/hashicorp/memberlist"
	"log"
	"math"
	"sort"
	"strconv"
	"sync"
	"time"
)

type Partitions struct {
	partitions map[int]*Partition
	sync.Mutex
}

type Partition struct {
	Id       int
	LastUsed time.Time
	sync.Mutex
}

func InitPartitions(cfg Config) Partitions {
	part := Partitions{
		partitions: make(map[int]*Partition),
	}
	//make sure there is an element in the array
	part.makePartitions(cfg, cfg.Core.InitPartitions)
	return part
}

func (part Partitions) PartitionCount() int {
	return len(part.partitions)
}

func (part Partitions) GetPartition(cfg Config, list *memberlist.Memberlist) (int, int, error) {

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
	partitionBottom := nodeStep * myPartition
	log.Println("my partition: " + strconv.Itoa(myPartition))
	log.Println("partitionBottom: " + strconv.Itoa(partitionBottom))
	partitionTop := nodeStep * (myPartition + 1)
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
func (part Partitions) getPartitionPosition(cfg Config) (int, int, error) {
	//iterate over the partitions and then increase or decrease the number of partitions

	//TODO move loging out of the sync operation for better throughput
	myPartition := -1
	occupiedPartitions := 0
	totalPartitions := len(part.partitions)
	var err error
	for partitionId := range part.partitions {
		//use visibility timeout of 30 seconds
		log.Println("partition: " + strconv.Itoa(partitionId) + "occupied time: " + strconv.FormatFloat(time.Since(part.partitions[partitionId].LastUsed).Seconds(), 'f', -1, 64))
		part.partitions[partitionId].Lock()
		if time.Since(part.partitions[partitionId].LastUsed).Seconds() > cfg.Core.Visibility {
			if myPartition == -1 {
				myPartition = partitionId
				part.partitions[partitionId].LastUsed = time.Now()

			}
		} else {
			occupiedPartitions = occupiedPartitions + 1
		}
		part.partitions[partitionId].Unlock()
	}
	//if I haven't found an unoccupied partition create more
	if myPartition == -1 {

		err = part.makePartitions(cfg, cfg.Core.PartitionStep)
		if err == nil {
			myPartition, totalPartitions, err = part.getPartitionPosition(cfg)
		}
	}
	log.Println("totalPartitions:" + strconv.Itoa(totalPartitions))
	log.Println("occupiedPartitions:" + strconv.Itoa(occupiedPartitions))
	return myPartition, totalPartitions, err
}

func (part Partitions) makePartitions(cfg Config, partitionsToMake int) error {
	part.Lock()
	defer part.Unlock()
	offset := len(part.partitions)
	var initialTime time.Time
	partitionsMade := 0
	for partitionId := offset; partitionId < offset+partitionsToMake; partitionId++ {
		if cfg.Core.MaxPartitions > partitionId {
			partition := new(Partition)
			partition.Id = partitionId
			partition.LastUsed = initialTime
			part.partitions[partitionId] = partition
			partitionsMade = partitionsMade + 1
		}
	}
	log.Println("tried to make " + strconv.Itoa(partitionsToMake))
	log.Println("made " + strconv.Itoa(partitionsMade))
	if partitionsMade != partitionsToMake {
		return errors.New("no available partitions")
	}
	return nil

}
