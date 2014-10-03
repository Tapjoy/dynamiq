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
}
type Queue struct {
	// the definition of a queue
	// name of the queue
	Name string
	// the partitions of the queue
	Parts Partitions
	//RiakPool chan *riak.Client
}

//lazy load the queues
func InitQueues() Queues {
	queues := Queues{
		QueueMap: make(map[string]Queue),
	}
	return queues
}

func (queues Queues) InitQueue(name string) {
	queues.QueueMap[name] = Queue{
		Name:  name,
		Parts: InitPartitions(),
		//	RiakPool: make(chan *riak.Client, 4096)
	}
}

// get a message from the queue
func (queue Queue) Get(cfg Config, list *memberlist.Memberlist, batchsize uint32) []riak.RObject {
	// get the top and bottom partitions
	partBottom, partTop := queue.Parts.GetPartition(cfg, list)

	// grab a riak client
	client := GetConn()
	defer PutConn(client)

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
	return queue.RetrieveMessages(messageIds)
}

// Put a Message onto the queue
// TO DO remove the message instantiation from the httpinterface
func (queue Queue) Put(message string) string {
	//Grab our bucket
	client := GetConn()
	defer PutConn(client)
	err := client.Connect()
	if err == nil {
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
		}
	}
	// We do not handle errors, these should either be logged, or returned to the client
	// TODO should through an error if we get here
	return ""
}

// Delete a Message from the queue
func (queue Queue) Delete(id string) bool {
	client := GetConn()
	defer PutConn(client)
	err := client.Connect()
	if err == nil {
		bucket, err := client.NewBucket(queue.Name)
		if err == nil {
			log.Println("Deleting: ", id)
			bucket.Delete(id)
			return true
		}
	}
	// if we got here we're borked
	return false
}

// helpers
func (queue Queue) RetrieveMessages(ids []string) []riak.RObject {
	var rObjectArrayChan = make(chan []riak.RObject, len(ids))
	var rKeys = make(chan string, len(ids))

	start := time.Now()
	//fmt.Println("In get multi")
	for i := 0; i < len(ids); i++ {
		go func() {
			var riakKey string
			client := GetConn()
			defer PutConn(client)
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

// Question, should the Riak connection pool be tied to the Queues type, or moved into its own area

// get a connection from the pool
var riakPool chan *riak.Client

//Get the a riak connection from the pool
func GetConn() *riak.Client {
	if riakPool == nil {
		//fmt.Println("Initializing client pool")
		riakPool = make(chan *riak.Client, 4096)
		for i := 0; i < 4096; i++ {
			//fmt.Println("Initializing client pool ", i)
			client, _ := NewClient()
			client.Ping()
			PutConn(client)
		}
	}
	conn := <-riakPool
	return conn
}

//put a riak connection back on the pool
func PutConn(conn *riak.Client) {
	if riakPool == nil {
		riakPool = make(chan *riak.Client, 4096)
	}
	//log.Printf("Conn backlog %v", len(riakPool))
	riakPool <- conn
}

//todo add this to the config file
func NewClient() (*riak.Client, string) {
	rand.Seed(time.Now().UnixNano())
	hosts := []string{"10.0.0.108:8087", "10.0.0.155:8087", "10.0.0.165:8087", "10.0.0.187:8087", "10.0.0.208:8087", "10.0.0.212:8087", "10.0.0.253:8087", "10.0.0.30:8087", "10.0.0.47:8087", "10.0.0.71:8087", "10.0.0.86:8087", "10.0.0.97:8087"}
	host := hosts[rand.Intn(len(hosts))]
	client := riak.NewClient(host)
	client.SetConnectTimeout(2 * time.Second)
	err := client.Connect()
	if err != nil {
		log.Println("Connect error")
		return NewClient()
	} else {
		return client, host
	}
}
