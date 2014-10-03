package app

import "log"
import "github.com/hashicorp/memberlist"
import "strconv"
import "time"
import "sort"
import "sync"

type Partitions struct {
	partitions map[int]time.Time
	sync.Mutex
}

func InitPartitions() Partitions {
	part := Partitions{
		partitions: make(map[int]time.Time),
	}
	//make sure there is an element in the array
	part.partitions[0] = time.Now()
	return part
}

func (part Partitions) GetPartition(cfg Config, list *memberlist.Memberlist) (int, int) {

	//get the node position and the node count
	nodePosition, nodeCount := getNodePosition(list)
	log.Println("Node Position: " + strconv.Itoa(nodePosition))
	log.Println("Node count: " + strconv.Itoa(nodeCount))

	//calculate the range that our node is responsible for
	step := cfg.Core.RingSize / nodeCount
	nodeBottom := nodePosition * step
	nodeTop := (nodePosition + 1) * step
	log.Println("Node Bottom: " + strconv.Itoa(nodeBottom))
	log.Println("Node Top: " + strconv.Itoa(nodeTop))
	//iterate over the partitions and then increase or decrease the number of partitions
	//start sync

	part.Lock()
	myPartition := -1
	occupiedPartitions := 0
	totalPartitions := len(part.partitions)
	for partition := range part.partitions {
		//use visibility timeout of 30 seconds
		log.Println("partition: " + strconv.Itoa(partition) + "occupied time: " + strconv.FormatFloat(time.Since(part.partitions[partition]).Seconds(), 'f', -1, 64))
		if time.Since(part.partitions[partition]).Seconds() > cfg.Core.Visibility {
			if myPartition == -1 {
				myPartition = partition
				part.partitions[partition] = time.Now()

			}
		} else {
			occupiedPartitions = occupiedPartitions + 1
		}
	}
	//if I haven't found an unoccupied partition add one
	if myPartition == -1 {
		new_partition := totalPartitions
		totalPartitions = totalPartitions + 1
		occupiedPartitions = occupiedPartitions + 1
		part.partitions[new_partition] = time.Now()
		myPartition = new_partition
	}
	//end sync
	part.Unlock()
	log.Println("totalPartitions:" + strconv.Itoa(totalPartitions))
	log.Println("occupiedPartitions:" + strconv.Itoa(occupiedPartitions))

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
	return partitionBottom, partitionTop

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
