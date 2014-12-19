package app

import (
	"fmt"
	"github.com/Tapjoy/dynamiq/app/stats"
	"github.com/hashicorp/memberlist"
	"github.com/tpjg/goriakpbc"
	"log"
	"math"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

// Define statistics keys suffixes

const QUEUE_SENT_STATS_SUFFIX = "sent.count"
const QUEUE_RECEIVED_STATS_SUFFIX = "received.count"
const QUEUE_DELETED_STATS_SUFFIX = "deleted.count"
const QUEUE_DEPTH_STATS_SUFFIX = "depth.count"
const QUEUE_INFLIGHT_STATS_SUFFIX = "inflight.count"
const QUEUE_DEPTHAPR_STATS_SUFFIX = "approximate_depth.count"

type Queues struct {
	// a container for all queues
	QueueMap map[string]*Queue
	// Settings for Queues in general, ie queue list
	Config *riak.RDtMap
	// Mutex for protecting rw access to the Config object
	sync.RWMutex
}

type Queue struct {
	// the definition of a queue
	// name of the queue
	Name string
	// the partitions of the queue
	Parts *Partitions
	// Individual settings for the queue
	Config *riak.RDtMap
	// Mutex for protecting rw access to the Config object
	sync.RWMutex
}

func incrementMessageCount(c stats.StatsClient, queueName string, numberOfMessages int64) error {
	// Increment # Sent
	key := fmt.Sprintf("%s.%s", queueName, QUEUE_SENT_STATS_SUFFIX)
	err := c.Incr(key, numberOfMessages)
	// Increment Depth count
	key = fmt.Sprintf("%s.%s", queueName, QUEUE_DEPTH_STATS_SUFFIX)
	err = c.IncrGauge(key, numberOfMessages)
	return err
}

func decrementMessageCount(c stats.StatsClient, queueName string, numberOfMessages int64) error {
	// Increment # Deleted
	key := fmt.Sprintf("%s.%s", queueName, QUEUE_DELETED_STATS_SUFFIX)
	err := c.Incr(key, numberOfMessages)
	// Decrement Inflight count
	key = fmt.Sprintf("%s.%s", queueName, QUEUE_INFLIGHT_STATS_SUFFIX)
	err = c.DecrGauge(key, numberOfMessages)
	// Decrement Depth count
	key = fmt.Sprintf("%s.%s", queueName, QUEUE_DEPTH_STATS_SUFFIX)
	err = c.DecrGauge(key, numberOfMessages)
	return err
}

func incrementReceiveCount(c stats.StatsClient, queueName string, numberOfMessages int64) error {
	// Increment # Received
	key := fmt.Sprintf("%s.%s", queueName, QUEUE_RECEIVED_STATS_SUFFIX)
	err := c.Incr(key, numberOfMessages)
	// Increment Inflight count
	key = fmt.Sprintf("%s.%s", queueName, QUEUE_INFLIGHT_STATS_SUFFIX)
	err = c.IncrGauge(key, numberOfMessages)
	return err
}
func (queue *Queue) setQueueDepthApr(c stats.StatsClient, list *memberlist.Memberlist, queueName string, ids []string) error {
	// set  depth
	key := fmt.Sprintf("%s.%s", queueName, QUEUE_DEPTHAPR_STATS_SUFFIX)
	// find the difference between the first messages id and the last messages id

	if len(ids) > 1 {
		first, _ := strconv.ParseInt(ids[0], 10, 64)
		last, _ := strconv.ParseInt(ids[len(ids)-1], 10, 64)
		difference := last - first
		//find the density of messages
		density := float64(len(ids)) / float64(difference)
		// find the total count of messages by multiplying the density by the key range
		count := density * math.MaxInt64
		return c.SetGauge(key, int64(count))

	} else {
		// for small queues where we only return 1  message or no messages guesstimate ( or should we return 0? )
		multiplier := queue.Parts.PartitionCount() * len(list.Members())
		return c.SetGauge(key, int64(len(ids)*multiplier))
	}
}

func (queues *Queues) Exists(cfg *Config, queueName string) bool {
	// For now, lets go right to Riak for this
	// Because of the config delay, we don't wanna check the memory values
	client := cfg.RiakConnection()
	defer cfg.ReleaseRiakConnection(client)

	bucket, _ := client.NewBucketType("maps", CONFIGURATION_BUCKET)
	m, _ := bucket.FetchMap(QUEUE_CONFIG_NAME)
	set := m.AddSet(QUEUE_SET_NAME)

	for _, value := range set.GetValue() {
		log.Printf("Looking for %s, found %s", queueName, string(value[:]))
		if string(value[:]) == queueName {
			return true
		}
	}
	return false
}

// get a message from the queue
func (queue *Queue) Get(cfg *Config, list *memberlist.Memberlist, batchsize uint32) ([]riak.RObject, error) {
	// get the top and bottom partitions
	partBottom, partTop, err := queue.Parts.GetPartition(cfg, queue.Name, list)

	if err != nil {
		return nil, err
	}
	// grab a riak client
	client := cfg.RiakConnection()
	defer cfg.ReleaseRiakConnection(client)

	//set the bucket
	bucket, err := client.NewBucket(queue.Name)
	if err != nil {
		log.Printf("Error%v", err)
	}
	//get a list of batchsize message ids
	messageIds, _, err := bucket.IndexQueryRangePage("id_int", strconv.Itoa(partBottom), strconv.Itoa(partTop), batchsize, "")
	defer queue.setQueueDepthApr(cfg.Stats.Client, list, queue.Name, messageIds)

	if err != nil {
		log.Printf("Error%v", err)
	}
	messageCount := len(messageIds)
	defer incrementReceiveCount(cfg.Stats.Client, queue.Name, int64(messageCount))
	log.Println("Message retrieved ", messageCount)
	return queue.RetrieveMessages(messageIds, cfg), err
}

// Put a Message onto the queue
func (queue *Queue) Put(cfg *Config, message string) string {
	//Grab our bucket
	client := cfg.RiakConnection()
	defer cfg.ReleaseRiakConnection(client)
	bucket, err := client.NewBucket(queue.Name)
	if err == nil {
		//Retrieve a UUID
		rand.Seed(time.Now().UnixNano())
		randInt := rand.Int63n(math.MaxInt64)
		uuid := strconv.FormatInt(randInt, 10)

		messageObj := bucket.NewObject(uuid)
		messageObj.Indexes["id_int"] = []string{uuid}
		messageObj.Data = []byte(message)
		messageObj.Store()

		defer incrementMessageCount(cfg.Stats.Client, queue.Name, 1)
		return uuid
	} else {
		//Actually want to handle this in some other way
		return ""
	}
}

// Delete a Message from the queue
func (queue *Queue) Delete(cfg *Config, id string) bool {
	client := cfg.RiakConnection()
	defer cfg.ReleaseRiakConnection(client)
	bucket, err := client.NewBucket(queue.Name)
	if err == nil {
		err = bucket.Delete(id)
		if err == nil {
			defer decrementMessageCount(cfg.Stats.Client, queue.Name, 1)
			return true
		}
	}
	if err != nil {
		log.Println(err)
	}
	// if we got here we're borked
	// TODO stats cleanup? Possibility that this gets us out of sync
	return false
}

// helpers
func (queue *Queue) RetrieveMessages(ids []string, cfg *Config) []riak.RObject {
	var rObjectArrayChan = make(chan []riak.RObject, len(ids))
	var rKeys = make(chan string, len(ids))

	start := time.Now()
	//fmt.Println("In get multi")
	for i := 0; i < len(ids); i++ {
		go func() {
			var riakKey string
			client := cfg.RiakConnection()
			defer cfg.ReleaseRiakConnection(client)
			//fmt.Println("Getting bucket")
			bucket, _ := client.NewBucket(queue.Name)
			riakKey = <-rKeys
			//fmt.Println("Getting value")
			rObject, _ := bucket.Get(riakKey)

			rObjectArrayChan <- []riak.RObject{*rObject}

			//fmt.Println("Returning value")
		}()
		rKeys <- ids[i]
	}
	returnVals := make([]riak.RObject, 0)
	for i := 0; i < len(ids); i++ {
		var rObjectArray = <-rObjectArrayChan
		//If the key isn't blank, we've got a meaningful object to deal with
		if len(rObjectArray) == 1 {
			returnVals = append(returnVals, rObjectArray[0])
		}
	}
	elapsed := time.Since(start)
	log.Printf("Get Multi Took %s\n", elapsed)
	return returnVals
}

func (queues *Queues) syncConfig(cfg *Config) {
	for {
		log.Println("syncing Queue config with Riak")
		client := cfg.RiakConnection()
		bucket, err := client.NewBucketType("maps", CONFIGURATION_BUCKET)
		if err != nil {
			log.Println(err)
		}

		queuesConfig, err := bucket.FetchMap(QUEUE_CONFIG_NAME)
		if err != nil {
			log.Println(err)
		}
		queues.updateQueuesConfig(queuesConfig)

		//iterate the map and add or remove topics that need to be destroyed
		queueSet := queues.getQueuesConfig().AddSet(QUEUE_SET_NAME)

		if queueSet == nil {
			//bail if there aren't any queues
			//but not before sleeping
			cfg.ReleaseRiakConnection(client)
			time.Sleep(cfg.Core.SyncConfigInterval * time.Second)
			continue
		}
		queueSlice := queueSet.GetValue()
		if queueSlice == nil {
			//bail if there aren't any queues
			//but not before sleeping
			cfg.ReleaseRiakConnection(client)
			time.Sleep(cfg.Core.SyncConfigInterval * time.Second)
			continue
		}

		//Is there a better way to do this?
		//iterate over the queues in riak and add the missing ones
		queuesToKeep := make(map[string]bool)
		for _, queue := range queueSlice {
			queueName := string(queue)
			var present bool
			_, present = queues.QueueMap[queueName]
			if present != true {
				initQueueFromRiak(cfg, queueName)
			}
			queuesToKeep[queueName] = true
		}

		//iterate over the topics in topics.TopicMap and delete the ones no longer used
		for queue, _ := range queues.QueueMap {
			var present bool
			_, present = queuesToKeep[queue]
			if present != true {
				delete(queues.QueueMap, queue)
			}
		}

		//sync all topics with riak

		for _, queue := range queues.QueueMap {
			queue.syncConfig(cfg)
		}
		//sleep for the configured interval
		cfg.ReleaseRiakConnection(client)
		time.Sleep(cfg.Core.SyncConfigInterval * time.Millisecond)
	}
}

func initQueueFromRiak(cfg *Config, queueName string) {
	client := cfg.RiakConnection()
	defer cfg.ReleaseRiakConnection(client)

	bucket, _ := client.NewBucketType("maps", CONFIGURATION_BUCKET)
	config, _ := bucket.FetchMap(queueConfigRecordName(queueName))

	queue := Queue{
		Name:   queueName,
		Parts:  InitPartitions(cfg, queueName),
		Config: config,
	}

	// This is adding a new member to the collection, it shouldn't need a lock?
	// TODO Keep an eye on this for emergent issues
	cfg.Queues.QueueMap[queueName] = &queue
}

func (queue *Queue) syncConfig(cfg *Config) {
	//refresh the queue RDtMap
	client := cfg.RiakConnection()
	defer cfg.ReleaseRiakConnection(client)
	bucket, _ := client.NewBucketType("maps", CONFIGURATION_BUCKET)

	rCfg, _ := bucket.FetchMap(queueConfigRecordName(queue.Name))
	queue.updateQueueConfig(rCfg)
	queue.Parts.syncPartitions(cfg, queue.Name)
}

func (queue *Queue) updateQueueConfig(rCfg *riak.RDtMap) {
	queue.Lock()
	defer queue.Unlock()
	queue.Config = rCfg
}

func (queue *Queue) getQueueConfig() *riak.RDtMap {
	queue.RLock()
	defer queue.RUnlock()
	return queue.Config
}

func (queues *Queues) updateQueuesConfig(rCfg *riak.RDtMap) {
	queues.Lock()
	defer queues.Unlock()
	queues.Config = rCfg
}

func (queues *Queues) getQueuesConfig() *riak.RDtMap {
	queues.RLock()
	defer queues.RUnlock()
	return queues.Config
}
