package partitioner

import (
	"github.com/Sirupsen/logrus"
	"sync"
	"time"
)

type RingEntry struct {
	value     int64
	timeStamp time.Time
}

type TimedRingCache struct {
	sync.Mutex
	ring               []RingEntry
	expirationDuration time.Duration
	gcInterval         time.Duration
}

func NewTimedRingCache(expirationDuration time.Duration, gcInterval time.Duration) *TimedRingCache {
	trc := &TimedRingCache{
		expirationDuration: expirationDuration,
		gcInterval:         gcInterval,
	}
	trc.beginExpiring()
	return trc
}

func (rc *TimedRingCache) push(value int64, timeStamp time.Time) {
	entry := RingEntry{
		value:     value,
		timeStamp: timeStamp,
	}

	rc.ring = append(rc.ring, entry)
}

func (rc *TimedRingCache) LockedRange() (int64, int64) {
	rc.Lock()
	defer rc.Unlock()
	if len(rc.ring) >= 1 {
		return rc.ring[0].value, rc.ring[len(rc.ring)-1].value
	} else {
		return 0, 0
	}
}

func (rc *TimedRingCache) ReserveRange(lowerLimit int64, upperLimit int64, rangeIncrease int64) (int64, int64) {
	rc.Lock()
	defer rc.Unlock()

	var lowerBound int64 = 0
	var upperBound int64 = 0

	if len(rc.ring) == 0 {
		logrus.Info("LEN 0!!!!!!")
		// First entry, they get what they want
		lowerBound = lowerLimit
		upperBound = lowerBound + rangeIncrease
		if upperBound > upperLimit {
			logrus.Info("UPPERBOUND > UPPERLIMIT!!!!!!")
			upperBound = upperLimit
		}
		logrus.Info("ABOUT TO PUSH", upperBound)
		rc.push(upperBound, time.Now())
		return lowerBound, upperBound
	}

	head := rc.ring[len(rc.ring)-1].value
	tail := rc.ring[0].value

	if len(rc.ring) == 1 {
		logrus.Info("LEN 1!!!!!!", head, tail)
		lowerBound = head + 1
		//
		if head == upperLimit {
			lowerBound = lowerLimit
		}
		upperBound = lowerBound + rangeIncrease
		if upperBound > upperLimit {
			logrus.Info("UPPERBOUND > UPPERLIMIT!!!!!!")
			upperBound = upperLimit
		}
	} else {
		// Tail will likely never be "lowerLimit", but it will possibly be the amount
		// of the rangeIncrease +1 due to what happens above
		if head == upperLimit && (tail == lowerLimit || tail == rangeIncrease+1) {
			// fully locked
			logrus.Info("FULLY LOCKED!!!!!!!")
			return -1, -1
		}

		if head < tail || (head > tail && head == upperLimit) {
			logrus.Info("HEAD < TAIL!!!!!", head, tail)
			// We were uppbounded, the tail started to fall off, we can loop the tail ranges
			// back onto the head.
			// Grow from head + 1 to to tail - 1, by rangeIncrease

			//This isn't working for the following case: we've hit upperLimit
			// but haven't yet begun wrapping. The head == upperlimit gets it close
			// still seeing 0,0 ranges, ranges that aren't right (off by -1)
			lowerBound = head + 1
			if head == upperLimit {
				lowerBound = lowerLimit
			}
			upperBound = lowerBound + rangeIncrease
			if upperBound > tail-1 {
				logrus.Info("UPPERBOUND > TAIL-1!!!!!")
				upperBound = tail - 1
			}
		} else if head > tail {
			logrus.Info("HEAD > TAIL!!!!!", head, tail)
			// There was room at the head for future growth
			// Grow from head + 1 to upperLimit by rangeIncrease
			lowerBound = head + 1
			upperBound = lowerBound + rangeIncrease
			if upperBound > upperLimit {
				logrus.Info("UPPERBOUND > UPPERLIMIT!!!!!")
				upperBound = upperLimit
			}
		}
	}
	logrus.Info("ABOUT TO PUSH", upperBound)
	rc.push(upperBound, time.Now())
	return lowerBound, upperBound
}

func (rc *TimedRingCache) ExpireRange(lowerBound int64, upperBound int64) {
	rc.Lock()
	defer rc.Unlock()
	position := -1
	for i, x := range rc.ring {
		if x.value == upperBound {
			position = i
		}
	}

	if position != -1 {
		rc.ring[position].timeStamp = time.Time{}
	}
}

func (rc *TimedRingCache) beginExpiring() {
	go func() {
		for _ = range time.Tick(rc.gcInterval) {
			//logrus.Info("Checking expiration...", time.Now(), len(rc.ring))
			if len(rc.ring) > 0 && time.Since(rc.ring[0].timeStamp) >= rc.expirationDuration {
				rc.Lock()
				// This is effectively a pop off of the head, pos 0, of a slice
				for len(rc.ring) > 0 && time.Since(rc.ring[0].timeStamp) >= rc.expirationDuration {
					// If the expirationDuration has expired, pop it
					// Keep going until nothing is expired or we're at 0
					rc.ring = append(rc.ring[:0], rc.ring[1:]...)
					logrus.Info("Expiration Occured!")
				}
				rc.Unlock()
			}
		}
	}()
}
