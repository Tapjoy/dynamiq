package partitioner

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

type ClassicPartitioner struct {
	sync.RWMutex

	MinPartitions     int
	MaxPartitions     int
	VisibilityTimeout float64
	Nodes             *memberlist.Memberlist
	partitionCount    int
	partitions        *lane.PQueue
}

type Partition struct {
	Id       int
	LastUsed time.Time
}

func NewClassicPartitioner(list *memberlist.Memberlist, minPartitions int, maxPartitions int, visibilityTimeout float64) *ClassicPartitioner {
	partitioner := &ClassicPartitioner{
		Nodes:             list,
		MinPartitions:     minPartitions,
		MaxPartitions:     maxPartitions,
		VisibilityTimeout: visibilityTimeout,
		partitionCount:    0,
		partitions:        lane.NewPQueue(lane.MINPQ),
	}
	partitioner.makePartitions(minPartitions)
	return partitioner
}

func (r *ClassicPartitioner) makePartitions(partitionsToMake int) {
	var initialTime time.Time
	offset := r.partitionCount
	for partitionId := offset; partitionId < offset+partitionsToMake; partitionId++ {
		if r.MaxPartitions > partitionId {
			partition := new(Partition)
			partition.Id = partitionId
			partition.LastUsed = initialTime
			r.partitions.Push(partition, rand.Int63n(100000))
			r.partitionCount = r.partitionCount + 1
		}
	}
}

func (r *ClassicPartitioner) getMyPosition() (int64, int64) {
	nodes := r.Nodes.Members()
	var nodeNames []string
	for _, node := range nodes {
		nodeNames = append(nodeNames, node.Name)
	}

	sort.Strings(nodeNames)

	myPos := sort.SearchStrings(nodeNames, r.Nodes.LocalNode().Name)
	return int64(myPos), int64(len(nodeNames))
}

func (r *ClassicPartitioner) getPartitionPosition() (int64, *Partition, int64, error) {
	//iterate over the partitions and then increase or decrease the number of partitions

	//TODO move loging out of the sync operation for better throughput
	myPartition := -1

	var err error
	poppedPartition, _ := r.partitions.Pop()
	var workingPartition *Partition
	if poppedPartition != nil {
		workingPartition = poppedPartition.(*Partition)
	} else {
		// this seems a little scary
		return int64(myPartition), workingPartition, int64(r.partitionCount), errors.New(NOPARTITIONS)
	}
	if time.Since(workingPartition.LastUsed).Seconds() > r.VisibilityTimeout {
		myPartition = workingPartition.Id
	} else {
		r.partitions.Push(workingPartition, workingPartition.LastUsed.UnixNano())
		r.Lock()
		defer r.Unlock()
		if r.partitionCount < r.MaxPartitions {
			workingPartition = new(Partition)
			workingPartition.Id = r.partitionCount
			myPartition = workingPartition.Id
			r.partitionCount = r.partitionCount + 1
		} else {
			err = errors.New(NOPARTITIONS)
		}
	}
	return int64(myPartition), workingPartition, int64(r.partitionCount), err
}

func (r *ClassicPartitioner) pushPartition(partition *Partition, lock bool) {
	if lock {
		partition.LastUsed = time.Now()
		r.partitions.Push(partition, partition.LastUsed.UnixNano())
	} else {
		partition.LastUsed = time.Now().Add(-(time.Duration(r.VisibilityTimeout) * time.Second))
		r.partitions.Push(partition, partition.LastUsed.UnixNano())
	}
}

func (r *ClassicPartitioner) GetRange(lockIncrement int64) (int64, int64, int, error) {
	//get the node position and the node count
	myPosition, nodeCount := r.getMyPosition()

	//calculate the range that our node is responsible for
	nodeKeyspaceSize := math.MaxInt64 / nodeCount
	nodeBottom := myPosition * nodeKeyspaceSize
	nodeTop := (myPosition + 1) * nodeKeyspaceSize

	myPartition, partition, totalPartitions, err := r.getPartitionPosition()

	if err != nil && err.Error() != NOPARTITIONS {
		logrus.Error(err)
	}

	// calculate my range for the given number
	node_range := nodeTop - nodeBottom
	nodeStep := node_range / totalPartitions
	partitionBottom := nodeStep*myPartition + nodeBottom
	partitionTop := nodeStep*(myPartition+1) + nodeBottom
	return partitionBottom, partitionTop, partition.Id, err

}

func (r *ClassicPartitioner) ExpireRange(lowerBound int64, upperBound int64, ordinal int) {
	p := &Partition{
		Id:       ordinal,
		LastUsed: time.Time{},
	}
	r.pushPartition(p, false)
}
