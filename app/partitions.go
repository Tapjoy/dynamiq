package app

import (
  "errors"
  "github.com/hashicorp/memberlist"
  "github.com/oleiade/lane"
  "log"
  "math"
  "math/rand"
  "sort"
  "strconv"
  "time"
)

type Partitions struct {
  partitions *lane.PQueue
}

type Partition struct {
  Id       int
  LastUsed time.Time
}

func InitPartitions(cfg Config) Partitions {
  part := Partitions{
    partitions: lane.NewPQueue(lane.MINPQ),
  }
  //make sure there is an element in the array
  part.makePartitions(cfg, cfg.Core.InitPartitions)
  return part
}

func (part Partitions) PartitionCount() int {
  return part.partitions.Size()
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
func (part Partitions) getPartitionPosition(cfg Config) (int, int, error) {
  //iterate over the partitions and then increase or decrease the number of partitions

  //TODO move loging out of the sync operation for better throughput
  myPartition := -1
  totalPartitions := part.partitions.Size()
  log.Println("totalPartitions:" + strconv.Itoa(totalPartitions))

  var err error
  poppedPartition, _ := part.partitions.Pop()
  workingPartition := poppedPartition.(*Partition)
  log.Println("partition: " + strconv.Itoa(workingPartition.Id) + " occupied time: " + strconv.FormatFloat(time.Since(workingPartition.LastUsed).Seconds(), 'f', -1, 64))

  if time.Since(workingPartition.LastUsed).Seconds() > cfg.Core.Visibility {
    myPartition = workingPartition.Id
    workingPartition.LastUsed = time.Now()
    part.partitions.Push(workingPartition, workingPartition.LastUsed.UnixNano())
  } else {

    part.partitions.Push(workingPartition, workingPartition.LastUsed.UnixNano())
    err = part.makePartitions(cfg, cfg.Core.PartitionStep)
    if err == nil {
      myPartition, totalPartitions, err = part.getPartitionPosition(cfg)
    }
  }
  log.Println("totalPartitions:" + strconv.Itoa(totalPartitions))
  return myPartition, totalPartitions, err
}

func (part Partitions) makePartitions(cfg Config, partitionsToMake int) error {
  offset := part.partitions.Size()
  partitionsMade := 0
  for partitionId := offset; partitionId < offset+partitionsToMake; partitionId++ {
    timeOffSet := -1 * rand.Int63n(1000000000000000000)

    if cfg.Core.MaxPartitions > partitionId {
      partition := new(Partition)
      partition.Id = partitionId
      partition.LastUsed = time.Now().Add(time.Duration(timeOffSet))
      part.partitions.Push(partition, rand.Int63n(100000))
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
