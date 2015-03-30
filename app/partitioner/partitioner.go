package partitioner

type Partitioner interface {
	GetRange(lockIncrement int64) (int64, int64, error)
	ExpireRange(lowerBound int64, upperBound int64, ordinal int)
}
