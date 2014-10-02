package main

import (

  "github.com/tpjg/goriakpbc"
  "fmt"
  "time"
  "strconv"
  "math/rand"
  "sync"
  "math"
)

type MessageBag struct {
  visibleTime int64
  client *riak.Client
}

func main() {
  //Warm up the conn
  conn := GetConn()
  PutConn(conn)
  for i := 0; i < 0; i++ {
    go func() {
      messageBag := NewMessageBag()
      for j := 0; j < 10000000; j++ {
        _ = messageBag.Put("test_bucket", "Some simple message")
      }
    }()
  }
  for i := 0; i < 0; i++ {
    for j := 0; j < 10000; j++ {
      //go func() {
        messageBag := NewMessageBag()
        start := time.Now()
        fmt.Println("Starting get ", j)
        messageBag.Get("test_bucket", 250)
        elapsed := time.Since(start)
        fmt.Printf("Get took %s for %v\n", elapsed, j)
      //}()    
    }            
  }  
  WriterBenchmark(4096, 10000000, 512)
  ReaderBenchmark(250, 1000000, true, 10)
  fmt.Printf("Complete\n")
}
  
func rand_str(str_size int) string {
    alphanum := "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
    var bytes = make([]byte, str_size)
    for i, _ := range bytes {
        bytes[i] = alphanum[rand.Intn(len(alphanum))]
    }
    return string(bytes)
}

func WriterBenchmark(workerCount int, numMessages int, messageSize int) {
  //Process five messages at a time
  messageChan := make(chan string, workerCount)
  termSig := make(chan bool)
  var wg sync.WaitGroup
  for i := 0; i < workerCount; i++ {
    wg.Add(1)
    go WriterWorker(messageChan, termSig, &wg)
  }
  for i := 0; i < numMessages; i++ {
    message := rand_str(messageSize)
    messageChan <- message
  }
  for i := 0; i < workerCount; i++ {
    termSig <- true
  }
  wg.Wait()
}

func WriterWorker(messageChan chan string, termChan chan bool, wg *sync.WaitGroup) {
  messageBag := NewMessageBag()  

  active := true  
  for active == true {
    select {
      case msg := <- messageChan:    
        _ = messageBag.Put("test_bucket_3", msg)
      case sig := <- termChan:
        fmt.Println("Close it out", sig)      
        active = false
    }
  }  
  wg.Done()
}


func ReaderBenchmark(workerCount int, numReads int, processDeletes bool, batchSize uint32) {
  //Process five messages at a time
  readSig := make(chan uint32, workerCount)
  termSig := make(chan bool)
  var wg sync.WaitGroup
  for i := 0; i < workerCount; i++ {
    wg.Add(1)
    go ReaderWorker(readSig, termSig, processDeletes, &wg)
  }
  for i := 0; i < numReads; i++ {
    fmt.Println("Issuing read request #", i)
    readSig <- batchSize
  }
  for i := 0; i < workerCount; i++ {
    termSig <- true
  }
  wg.Wait()
}

func ReaderWorker(readSize chan uint32, termChan chan bool, processDeletes bool, wg *sync.WaitGroup) {  
  messageBag := NewMessageBag()
  active := true  
  for active {
    select {
      case batchSize := <- readSize:
        fmt.Println("Beginning read")
        messages := messageBag.Get("test_bucket_3", batchSize)        
        fmt.Println("Read complete")
        if processDeletes {
          for _, message := range messages {
            messageBag.Delete("test_bucket_3", message.Key)
          }
        }
      case sig := <- termChan:
        fmt.Println("Close it out", sig)      
        active = false
    }
  }  
  wg.Done()
}

/* Begin Message Bag Def */

func NewMessageBag() *MessageBag {  
  return &MessageBag{visibleTime: (1000000000 * 60)}
}

func NewClient() (*riak.Client, string) {
  rand.Seed(time.Now().UnixNano())
  hosts := []string{"10.0.0.108:8087", "10.0.0.155:8087", "10.0.0.165:8087", "10.0.0.187:8087", "10.0.0.208:8087", "10.0.0.212:8087", "10.0.0.253:8087", "10.0.0.30:8087", "10.0.0.47:8087", "10.0.0.71:8087", "10.0.0.86:8087", "10.0.0.97:8087"}  
  host := hosts[rand.Intn(len(hosts))]
  client := riak.NewClient(host)
  client.SetConnectTimeout(2 * time.Second)
  err := client.Connect()
  if err != nil {
    fmt.Println("Connect error")
    return NewClient()
  } else {
    return client, host              
  }
}

/* Put message into bag */
func (m *MessageBag) Put(bagName string, message string) string {
  //Grab our bucket
  client := GetConn()
  defer PutConn(client)
  err := client.Connect()
  if err == nil {
    bucket, err := client.NewBucket(bagName)
    if err == nil {
      //Retrieve a UUID
      rand.Seed(time.Now().UnixNano())
      t := time.Now().UnixNano()
      randInt := rand.Int63n(math.MaxInt64)
      uuid := strconv.FormatInt(randInt, 10)

      messageObj := bucket.NewObject(uuid)
      messageObj.Indexes["id_int"] = []string{uuid}
      messageObj.Indexes["inflight_int"] = []string{strconv.FormatInt(t, 10)}
      messageObj.Data = []byte(message)
      messageObj.Store()
      return uuid
    } else {
      //Actually want to handle this in some other way
      return ""
    }
  } else {
    //Actually want to handle this in some other way
    return ""
  }
} 

/* Delete messages from bag */
func (m *MessageBag) Delete(bagName string, id string) string {
  client := GetConn()
  defer PutConn(client)
  err := client.Connect()
  if err == nil {
    bucket, err := client.NewBucket(bagName)
    if err == nil {
      fmt.Println("Deleting: ", id)
      bucket.Delete(id)
    }
  }
  return id
}

var riakPool chan *riak.Client
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
  conn := <- riakPool
  return conn
}

func PutConn(conn *riak.Client) {
  if riakPool == nil {
    riakPool = make(chan *riak.Client, 4096)
  }
  //fmt.Printf("Conn backlog %v", len(riakPool))
  riakPool <- conn
}

func (m *MessageBag) GetMulti(bagName string, ids []string) []riak.RObject {
  var rObjectArrayChan = make(chan []riak.RObject, len(ids))
  var rKeys = make(chan string, len(ids))

  start := time.Now()
  //fmt.Println("In get multi") 
  for i:=0; i < len(ids); i++ {
    go func() {
      var riakKey string
      client := GetConn()     
      defer PutConn(client)
      //fmt.Println("Getting bucket")
      bucket, _ := client.NewBucket(bagName)
      riakKey = <- rKeys
      //fmt.Println("Getting value")
      rObject, _ := bucket.Get(riakKey)

      //Inflight logic
      t := time.Now().UnixNano()
      /* If the inflight time has passed */
      var idxTime int64
      idxTime = 0
      if len(rObject.Indexes["inflight_int"]) == 1 {
        idxTime, _ = strconv.ParseInt(rObject.Indexes["inflight_int"][0], 10, 64)
      } else {
        rObject.Indexes = make(map[string][]string)
      }
      if idxTime < t + m.visibleTime {
        rObject.Indexes["inflight_int"] = []string{strconv.FormatInt(t, 10)}
        rObject.Indexes["id_int"] = []string{rObject.Key}
        rObject.Store()
        rObjectArrayChan <- []riak.RObject{*rObject}
      } else {
        fmt.Println("Index time doesn't line up")
        rObjectArrayChan <- []riak.RObject{}
      }

      //fmt.Println("Returning value")      
    }()
    rKeys <- ids[i]
  }
  returnVals := make([]riak.RObject, 0)
  for i:= 0; i < len(ids); i++ {      
    var rObjectArray = <- rObjectArrayChan
    //If the key isn't blank, we've got a meaningful object to deal with
    if len(rObjectArray) == 1 {
      returnVals = append(returnVals, rObjectArray[0])  
    }       
  }
  elapsed := time.Since(start)
  fmt.Printf("Get Multi Took %s\n", elapsed)
  return returnVals
}

func (m *MessageBag) PutMulti(bagName string, riakObjects []riak.RObject) {    
  var rObjectChan = make(chan riak.RObject, len(riakObjects))
  var completions = make(chan bool, len(riakObjects))
  start := time.Now()

  for i:=0; i < len(riakObjects); i++ {
    go func() {
      client := GetConn()     
      defer PutConn(client)        
      riakObject := <- rObjectChan
      //fmt.Printf("Putting riak object %v\n", riakObject)
      riakObject.Store()
      completions <- true  
    }()
    rObjectChan <- riakObjects[i]
  }

  for i:= 0; i < len(riakObjects); i++ {      
    _ = <- completions
    //fmt.Printf("\nPut Completed %v of %v\n", i, len(riakObjects) - 1)
  }
  elapsed := time.Since(start)
  fmt.Printf("\nPut Multi Took %s\n", elapsed)
}

func (m *MessageBag) Get(bagName string, batchSize uint32) []riak.RObject {
  var returnMessages []riak.RObject    
  client := GetConn()    
  defer PutConn(client)
  //fmt.Println("Getting bucket")
  bucket, err := client.NewBucket(bagName)
  //fmt.Println("Bucket retrieved")
  if err == nil {
    //now := time.Now()
    //t := now.UnixNano()      
    rand.Seed(time.Now().UnixNano())
    baseDec := rand.Float64()
    highDec := baseDec + (1.0 / 512.0)
    if highDec > 1 {
      highDec = 1
    }
    base :=  baseDec * math.MaxInt64
    max := highDec * math.MaxInt64
    //fmt.Printf("Base: %v & Max: %v\n", base, max)
    start := time.Now()
    //fmt.Println("Retrieving messages")
    messageIds, _, err := bucket.IndexQueryRangePage("id_int", strconv.FormatFloat(base, 'f', 0, 32), strconv.FormatFloat(max, 'f', 0, 32), batchSize, "")
    //fmt.Println("Message retrieved ", len(messageIds))
    elapsed := time.Since(start)
    fmt.Printf("id_int query took %s and returned %v messages \n", elapsed, len(messageIds))
    if err == nil {          
      returnMessages = m.GetMulti(bagName, messageIds)      
    } else {
      fmt.Printf("Error%v", err)
    }
  }
  
  return returnMessages
}

/* End Message Bag Def */