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

func Initpartitions() Partitions {
	part := Partitions{
		partitions: make(map[int]time.Time),
	}
	//make sure there is an element in the array
	part.partitions[0] = time.Now()
	return part
}

func (part Partitions) Getpartition(cfg Config, list *memberlist.Memberlist) (int, int) {

	//get the node position and the node count
	node_position, node_count := getnodeposition(list)
	log.Println("Node Position: " + strconv.Itoa(node_position))
	log.Println("Node count: " + strconv.Itoa(node_count))

	//calculate the range that our node is responsible for
	step := cfg.Core.Ringsize / node_count
	node_bottom := node_position * step
	node_top := (node_position + 1) * step
	log.Println("Node Bottom: " + strconv.Itoa(node_bottom))
	log.Println("Node Top: " + strconv.Itoa(node_top))
	//iterate over the partitions and then increase or decrease the number of partitions
	//start sync

	part.Lock()
	mypartition := -1
	occupied_partitions := 0
	total_partitions := len(part.partitions)
	for partition := range part.partitions {
		//use visibility timeout of 30 seconds
		log.Println("partition: " + strconv.Itoa(partition) + "occupied time: " + strconv.FormatFloat(time.Since(part.partitions[partition]).Seconds(), 'f', -1, 64))
		if time.Since(part.partitions[partition]).Seconds() > cfg.Core.Visibility {
			if mypartition == -1 {
				mypartition = partition
				part.partitions[partition] = time.Now()

			}
		} else {
			occupied_partitions = occupied_partitions + 1
		}
	}
	//if I haven't found an unoccupied partition add one
	if mypartition == -1 {
		new_partition := total_partitions
		total_partitions = total_partitions + 1
		occupied_partitions = occupied_partitions + 1
		part.partitions[new_partition] = time.Now()
		mypartition = new_partition
	}
	//end sync
	part.Unlock()
	log.Println("total_partitions:" + strconv.Itoa(total_partitions))
	log.Println("occupied_partitions:" + strconv.Itoa(occupied_partitions))

	// calculate my range for the given number
	node_range := node_top - node_bottom
	log.Println("Node Range: " + strconv.Itoa(node_range))
	node_step := node_range / total_partitions
	log.Println("node_step: " + strconv.Itoa(node_step))
	partition_bottom := node_step * mypartition
	log.Println("my partition: " + strconv.Itoa(mypartition))
	log.Println("partition_bottom: " + strconv.Itoa(partition_bottom))
	partition_top := node_step * (mypartition + 1)
	log.Println("partition_top: " + strconv.Itoa(partition_top))
	return partition_bottom, partition_top

}

//helper method to get the node position
func getnodeposition(list *memberlist.Memberlist) (int, int) {
	// figure out which node we are
	// grab and sort the node names
	nodes := list.Members()
	var node_names []string
	for _, node := range nodes {
		node_names = append(node_names, node.Name)
	}
	// sort our nodes so that we have a canonical ordering
	// node failure will cause more dupes
	sort.Strings(node_names)
	// find our index position
	node_position := sort.SearchStrings(node_names, list.LocalNode().Name)
	node_count := len(node_names)
	return node_position, node_count
}
