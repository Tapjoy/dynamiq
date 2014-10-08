package app

import (
	"github.com/hashicorp/memberlist"
	"github.com/tpjg/goriakpbc"
	"log"
	"math"
	"math/rand"
	"strconv"
	"time"
)

type Queues struct {
	// a container for all queues
	QueueMap map[string]Queue
	//container for all objects
	//should contain list of queues + global configuration parameters
	// Uses CRDTS :)
	config riak.RObject
	//container for global stats about all queues
	Stats riak.RObject
}
type Queue struct {
	// the definition of a queue
	// name of the queue
	Name string
	// the partitions of the queue
	Parts Partitions
	// statistics about a particular queue
	Stats riak.RObject
	// configuration of the queue
	Config riak.RObject
}

//lazy load the queues
func InitQueues() Queues {
	queues := Queues{
		QueueMap: make(map[string]Queue),
		config:   riak.RObject, // need to sort this out?
		Stats:    riak.RObject,
	}
	return queues
}

func (queues Queues) InitQueue(cfg Config, name string) {
	queues.QueueMap[name] = Queue{
		Name:  name,
		Parts: InitPartitions(cfg),
	}
}

// get a message from the queue
func (queue Queue) Get(cfg Config, list *memberlist.Memberlist, batchsize uint32, riakPool RiakPool) ([]riak.RObject, error) {
	// get the top and bottom partitions
	partBottom, partTop, err := queue.Parts.GetPartition(cfg, list)

	if err != nil {
		return nil, err
	}
	// grab a riak client
	client := riakPool.GetConn()
	defer riakPool.PutConn(client)

	//set the bucket
	bucket, err := client.NewBucket(queue.Name)
	if err != nil {
		log.Printf("Error%v", err)
	}
	//get a list of batchsize message ids
	messageIds, _, err := bucket.IndexQueryRangePage("id_int", strconv.Itoa(partBottom), strconv.Itoa(partTop), batchsize, "")

	if err != nil {
		log.Printf("Error%v", err)
	}
	log.Println("Message retrieved ", len(messageIds))
	return queue.RetrieveMessages(messageIds, cfg, riakPool), err
}

// Put a Message onto the queue
func (queue Queue) Put(cfg Config, message string, riakPool RiakPool) string {
	//Grab our bucket
	client := riakPool.GetConn()
	defer riakPool.PutConn(client)
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
		return uuid
	} else {
		//Actually want to handle this in some other way
		return ""
	}
}

// Delete a Message from the queue
func (queue Queue) Delete(cfg Config, id string, riakPool RiakPool) bool {
	client := riakPool.GetConn()
	defer riakPool.PutConn(client)
	bucket, err := client.NewBucket(queue.Name)
	if err == nil {
		log.Println("Deleting: ", id)
		bucket.Delete(id)
		return true
	}
	if err != nil {
		log.Println(err)
	}
	// if we got here we're borked
	return false
}

// helpers
func (queue Queue) RetrieveMessages(ids []string, cfg Config, riakPool RiakPool) []riak.RObject {
	var rObjectArrayChan = make(chan []riak.RObject, len(ids))
	var rKeys = make(chan string, len(ids))

	start := time.Now()
	//fmt.Println("In get multi")
	for i := 0; i < len(ids); i++ {
		go func() {
			var riakKey string
			client := riakPool.GetConn()
			defer riakPool.PutConn(client)
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
