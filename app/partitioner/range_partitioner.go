package partitioner

import (
	"github.com/hashicorp/memberlist"
	"math"
	"sort"
)

type RangePartitioner struct {
	Nodes     *memberlist.Memberlist
	ringCache *TimedRingCache
}

func NewRangePartitioner(list *memberlist.Memberlist, ringCache *TimedRingCache) *RangePartitioner {
	return &RangePartitioner{
		Nodes:     list,
		ringCache: ringCache,
	}
}

func (r *RangePartitioner) GetRange(lockIncrement int64) (int64, int64, int, error) {
	myPosition, nodeCount := r.getMyPosition()

	// Calculate the range that our node is responsible for
	// Do this every time, so that we don't cache bad values if the
	// cluster changes state
	nodeKeyspaceSize := math.MaxInt64 / nodeCount
	nodeBottom := myPosition * nodeKeyspaceSize
	nodeTop := (myPosition + 1) * nodeKeyspaceSize

	// Find the nearest locked range, if one exists
	lowerBound, upperBound := r.ringCache.ReserveRange(nodeBottom, nodeTop, lockIncrement)

	// Return the values
	return lowerBound, upperBound, 0, nil
}

func (r *RangePartitioner) ExpireRange(lowerBound int64, upperBound int64, ordinal int) {
	r.ringCache.ExpireRange(lowerBound, upperBound)
}

func (r *RangePartitioner) getMyPosition() (int64, int64) {
	nodes := r.Nodes.Members()
	var nodeNames []string
	for _, node := range nodes {
		nodeNames = append(nodeNames, node.Name)
	}

	sort.Strings(nodeNames)

	myPos := sort.SearchStrings(nodeNames, r.Nodes.LocalNode().Name)
	return int64(myPos), int64(len(nodeNames))
}
