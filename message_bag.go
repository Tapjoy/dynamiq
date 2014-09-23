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
    host string
    client *riak.Client
  }

  func main() {
    fmt.Println("In here yo")
    for i := 0; i < 0; i++ {
      go func() {
        messageBag := NewMessageBag()
        for j := 0; j < 1000000; j++ {
          _ = messageBag.Put("test_bucket", "Some simple message")    
        }
      }()
    } 
    for i := 0; i < 16; i++ {
      go func() {
        fmt.Println("In here yo")
        for j := 0; j < 500; j++ {
          messageBag := NewMessageBag()
          fmt.Println("In here yoz")
          start := time.Now()
          messageBag.Get("test_bucket")   
          elapsed := time.Since(start)
          fmt.Printf("Get took %s\n", elapsed)
        }      
      }()    
    }
    time.Sleep(100 * time.Second)
    fmt.Printf("Complete\n")
  }



  /* Begin Message Bag Def */

  func NewMessageBag() *MessageBag {  
    client, host := NewClient()
    return &MessageBag{visibleTime: (1000000000 * 300), host: host, client: client}
  }

  func NewClient() (*riak.Client, string) {
    rand.Seed(time.Now().UnixNano())
    hosts := []string{"10.0.0.86:8087", "10.0.0.212:8087", "10.0.0.187:8087", "10.0.0.30:8087", "10.0.0.208:8087", "10.0.0.97:8087", "10.0.0.71:8087", "10.0.0.155:8087", "10.0.0.47:8087", "10.0.0.108:8087", "10.0.0.165:8087", "10.0.0.253:8087"}  
    host := hosts[rand.Intn(12)]
    client := riak.NewClient(host)
    return client, host
  }

  /* Put message into bag */
  func (m *MessageBag) Put(bagName string, message string) string {
    //Grab our bucket
    client := riak.NewClient(m.host)
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
    client := riak.NewClient(m.host)
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
      riakPool = make(chan *riak.Client, 1000)
      for i := 0; i < 1000; i++ {
        client, _ := NewClient()
        PutConn(client)
      }
    }
    return <- riakPool
  }

  func PutConn(conn *riak.Client) {
    if riakPool == nil {
      riakPool = make(chan *riak.Client, 1000)
    }
    riakPool <- conn
  }

  func (m *MessageBag) GetMulti(bagName string, ids []string) []riak.RObject {
    var rObjects = make(chan riak.RObject, len(ids))
    var rKeys = make(chan string, len(ids))

    start := time.Now()
    
    for _, key := range ids {
      go func() {
        var riakKey string
        fmt.Println("Getting a conn")
        client := GetConn()        
        fmt.Println("Got a con")
        bucket, _ := client.NewBucket(bagName)
        riakKey = <- rKeys
        rObject, _ := bucket.Get(riakKey)
        fmt.Println("Got a key: %v", rObject)
        rObjects <- *rObject
        fmt.Println("Returning client")
        PutConn(client)
      }()
      rKeys <- key
    }
    fmt.Println("Here at return vals")
    returnVals := make([]riak.RObject, len(ids))
    for i:= 0; i < len(ids); i++ {
      fmt.Println("Waiting on an objct")    
      var obj = <- rObjects
      returnVals = append(returnVals, obj)
      fmt.Println("At %v with length %v", i, len(ids))
    }
    elapsed := time.Since(start)
    fmt.Printf("Get Multi Took %s\n", elapsed)
    return returnVals
  }

  func (m *MessageBag) Get(bagName string) []riak.RObject {
    messages := make([]riak.RObject, 200)  
    client := m.client
    err := client.Connect()
    if err == nil {    
      bucket, err := client.NewBucket(bagName)
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
        fmt.Println("Base: ", base)
        start := time.Now()
        messageIds, _, err := bucket.IndexQueryRangePage("id_int", strconv.FormatFloat(base, 'f', 0, 32), strconv.FormatFloat(max, 'f', 0, 32), 250, "")
        elapsed := time.Since(start)
        fmt.Printf("id_int query took %s\n", elapsed)
        length := len(messageIds)
        fmt.Println("Length ", length)    
        if err == nil {
          messages = m.GetMulti(bagName, messageIds)
        } else {
          fmt.Printf("Error%v", err)
        }
      }
    }
    return messages
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
        messageIds, _, err := bucket.IndexQueryRangePage("id_int", strconv.FormatFloat(base, 'f', 0, 32), strconv.FormatFloat(max, 'f', 0, 32), 250, "")
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