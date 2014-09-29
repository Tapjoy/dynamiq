package main

import (

  "github.com/tpjg/goriakpbc"
  "fmt"
  "time"
  "strconv"
  "math/rand"
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
      for j := 0; j < 1000000; j++ {
        _ = messageBag.Put("test_bucket", "Some simple message")    
      }
    }()
  }
  for i := 0; i < 1; i++ {
    for j := 0; j < 10000; j++ {
      //go func() {
        messageBag := NewMessageBag()
        start := time.Now()
        fmt.Println("Starting get ", j)
        messageBag.Get("test_bucket")   
        elapsed := time.Since(start)
        fmt.Printf("Get took %s for %v\n", elapsed, j)
      //}()    
    }            
  }
  time.Sleep(100 * time.Second)
  fmt.Printf("Complete\n")
}



/* Begin Message Bag Def */

func NewMessageBag() *MessageBag {  
  return &MessageBag{visibleTime: (1000000000 * 60)}
}

func NewClient() (*riak.Client, string) {
  rand.Seed(time.Now().UnixNano())
  hosts := []string{"ec2-54-68-213-20.us-west-2.compute.amazonaws.com:8087", "ec2-54-68-12-138.us-west-2.compute.amazonaws.com:8087", "ec2-54-69-47-53.us-west-2.compute.amazonaws.com:8087", "ec2-54-69-46-124.us-west-2.compute.amazonaws.com:8087", "ec2-54-68-49-228.us-west-2.compute.amazonaws.com:8087", "ec2-54-68-118-225.us-west-2.compute.amazonaws.com:8087", "ec2-54-69-49-89.us-west-2.compute.amazonaws.com:8087", "ec2-54-69-49-183.us-west-2.compute.amazonaws.com:8087", "ec2-54-69-50-238.us-west-2.compute.amazonaws.com:8087", "ec2-54-69-29-87.us-west-2.compute.amazonaws.com:8087", "ec2-54-69-49-114.us-west-2.compute.amazonaws.com:8087", "ec2-54-68-187-187.us-west-2.compute.amazonaws.com:8087"}  
  host := hosts[5]
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
      randInt := rand.Intn(9999999999999999)
      fmt.Println("Random int ", randInt)
      uuid := strconv.Itoa(randInt)

      messageObj := bucket.NewObject(uuid)
      messageObj.Indexes["id_int"] = []string{uuid}
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
      bucket.Delete(id)
    }
  }
  return id
}

var riakPool chan *riak.Client
func GetConn() *riak.Client {
  if riakPool == nil {
    fmt.Println("Initializing client pool")
    riakPool = make(chan *riak.Client, 20)
    for i := 0; i < 20; i++ {
      fmt.Println("Initializing client pool ", i)
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
    riakPool = make(chan *riak.Client, 20)
  }
  fmt.Printf("Conn backlog %v", len(riakPool))
  riakPool <- conn
}

func (m *MessageBag) GetMulti(bagName string, ids []string) []riak.RObject {
  var rObjects = make(chan riak.RObject, len(ids))
  var rKeys = make(chan string, len(ids))

  start := time.Now()
  fmt.Println("In get multi") 
  for i:=0; i < len(ids); i++ {
    go func() {
      var riakKey string
      client := GetConn()     
      defer PutConn(client)
      fmt.Println("Getting bucket")
      bucket, _ := client.NewBucket(bagName)
      riakKey = <- rKeys
      fmt.Println("Getting value")
      rObject, _ := bucket.Get(riakKey)
      fmt.Println("Returning value")
      rObjects <- *rObject      
    }()
    rKeys <- ids[i]
  }
  returnVals := make([]riak.RObject, 0)
  for i:= 0; i < len(ids); i++ {      
    var obj = <- rObjects      
    returnVals = append(returnVals, obj)
    fmt.Printf("\nGet Completed %v of %v", i, len(ids) - 1)
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
      fmt.Printf("Putting riak object %v\n", riakObject)
      riakObject.Store()
      completions <- true  
    }()
    rObjectChan <- riakObjects[i]
  }

  for i:= 0; i < len(riakObjects); i++ {      
    _ = <- completions
    fmt.Printf("\nPut Completed %v of %v\n", i, len(riakObjects) - 1)
  }
  elapsed := time.Since(start)
  fmt.Printf("\nPut Multi Took %s\n", elapsed)
}

func (m *MessageBag) Get(bagName string) []riak.RObject {
  var messages []riak.RObject
  var returnMessages []riak.RObject    
  client := GetConn()    
  defer PutConn(client)
  fmt.Println("Getting bucket")
  bucket, err := client.NewBucket(bagName)
  fmt.Println("Bucket retrieved")
  if err == nil {
    //now := time.Now()
    //t := now.UnixNano()      
    rand.Seed(time.Now().UnixNano())
    baseDec := rand.Float64()
    highDec := baseDec + (1.0 / 512.0)
    if highDec > 1 {
      highDec = 1
    }
    base :=  baseDec * 9999999999999999
    max := highDec * 9999999999999999
    fmt.Printf("Base: %v & Max: %v\n", base, max)
    start := time.Now()
    fmt.Println("Retrieving messages")
    messageIds, _, err := bucket.IndexQueryRangePage("id_int", strconv.FormatFloat(base, 'f', 0, 32), strconv.FormatFloat(max, 'f', 0, 32), 250, "")
    fmt.Println("Message retrieved ", len(messageIds))
    elapsed := time.Since(start)
    fmt.Printf("id_int query took %s and returned %v messages \n", elapsed, len(messageIds))
    length := len(messageIds)
    fmt.Println("Length ", length)    
    if err == nil {          
      messages = m.GetMulti(bagName, messageIds)
      for _, message := range messages {
        t := time.Now().UnixNano()
        /* If the inflight time has passed */
        var idxTime int64
        idxTime = 0
        if len(message.Indexes["inflight_int"]) == 1 {
          idxTime, _ = strconv.ParseInt(message.Indexes["inflight_int"][0], 10, 64)
        } else {
          message.Indexes = make(map[string][]string)
        }
        if idxTime < t - m.visibleTime {
          message.Indexes["inflight_int"] = []string{strconv.FormatInt(t, 10)}
          message.Indexes["id_int"] = []string{message.Key}
          returnMessages = append(returnMessages, message)            
        } else {
          // NOOP
        }
      }
      m.PutMulti(bagName, returnMessages)
    } else {
      fmt.Printf("Error%v", err)
    }
  }
  
  return returnMessages
}

func (m *MessageBag) GetWithInflight(bagName string) []*riak.RObject {
  messages := make([]*riak.RObject, 200)
  client := m.client
  err := client.Connect()
  if err == nil {
    bucket, err := client.NewBucket(bagName)
    if err == nil {
      now := time.Now()
      t := now.UnixNano()
      //Retrieve the inflight IDs
      start := time.Now()
      messageInFlightIds, err := bucket.IndexQueryRange("inflight_int", strconv.FormatInt(t - m.visibleTime, 10), strconv.FormatInt(t, 10))
      elapsed := time.Since(start)
      fmt.Printf("inflight_int query took %s %v\n", elapsed, len(messageInFlightIds))
      //Retrieve inflight + 1 so you guarantee getting something (or nothing, if nothing is available)
      //message_ids  = bag.get_index( 'id_bin', '0'..'z', max_results: (message_inflight_ids.count()+1))
      rand.Seed(time.Now().UnixNano())
      baseDec := rand.Float64()
      highDec := baseDec + (1.0 / 512.0)
      if highDec > 1 {
        highDec = 1
      }
      base :=  baseDec * 9999999999999999
      max := highDec * 9999999999999999
      fmt.Println("Base: ", base)
      start = time.Now()
      messageIds, _, err := bucket.IndexQueryRangePage("id_int", strconv.FormatFloat(base, 'f', 0, 32), strconv.FormatFloat(max, 'f', 0, 32), 1, "")
      elapsed = time.Since(start)
      fmt.Printf("id_int query took %s\n", elapsed)
      length := len(messageIds)
      fmt.Println("Length ", length)    
      messageSet := make(map[string]bool)
      targetIds := make([]string, 0, length)
      for i := 0; i < len(messageInFlightIds); i++ {
        messageSet[messageInFlightIds[i]] = true
      }

      for i := 0; i < len(messageIds); i++ {
        _, present := messageSet[messageIds[i]]
        if present == false {
          targetIds = append(targetIds, messageIds[i])
        }
      }
      if err == nil {
        //fmt.Printf("In flight IDs%v\n", messageInFlightIds)
        //fmt.Printf("Message Set IDs%v\n", messageSet)
        //fmt.Printf("Continuation %v\n", continuation)
        fmt.Printf("Target id length %v\n", len(targetIds))
        for _, messageId := range targetIds {
          go func(){
            message, _ := bucket.Get(messageId)
            message.Indexes["inflight_int"] = []string{strconv.FormatInt(t, 10)}
            message.Store()
            messages = append(messages, message)
          }()           
        }
      } else {
        fmt.Printf("Error%v", err)
      }
    }
  }
  return messages
}

/* End Message Bag Def */