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
}

func main() {
  messageBag := NewMessageBag()  
  for i := 0; i < 0; i++ {
    _ = messageBag.Put("test_bucket", "Some simple message")
  } 
  for i := 0; i < 2; i++ {
    go func() {
      for j := 0; j < 500; j++ {
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
  return &MessageBag{visibleTime: (1000000000 * 300), host: "127.0.0.1:8087"}
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

func (m *MessageBag) Get(bagName string) []*riak.RObject {
  messages := make([]*riak.RObject, 200)
  client := riak.NewClient(m.host)
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
      messageIds, continuation, err := bucket.IndexQueryRangePage("id_int", strconv.FormatFloat(base, 'f', 0, 32), strconv.FormatFloat(max, 'f', 0, 32), uint32(len(messageInFlightIds) + 1), "")
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
        fmt.Printf("Continuation %v\n", continuation)
        fmt.Printf("Target ids %v\n", targetIds)
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
