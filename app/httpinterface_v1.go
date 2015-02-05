package app

import (
	"bytes"
	"fmt"
	"github.com/Sirupsen/logrus"
	"github.com/go-martini/martini"
	"github.com/hashicorp/memberlist"
	"github.com/martini-contrib/binding"
	"github.com/martini-contrib/render"
	"net/http"
	"strconv"
	"time"
)

// TODO Should this live in the config package?
// Using pointers lets us differentiate between a natural 0, and an int-default 0
// omitempty tells it to ignore anything where the name was provided, but an empty value
// inside of a request body.
type ConfigRequest struct {
	VisibilityTimeout *float64 `json:"visibility_timeout,omitempty"`
	MinPartitions     *int     `json:"min_partitions,omitempty"`
	MaxPartitions     *int     `json:"max_partitions,omitempty"`
	MaxPartitionAge   *float64 `json:"max_partition_age,omitempty"`
}

// TODO make message definitions more explicit

func logrusLogger() martini.Handler {
	return func(res http.ResponseWriter, req *http.Request, c martini.Context, log *logrus.Logger) {
		start := time.Now()

		log.WithFields(logrus.Fields{
			"method": req.Method,
			"path":   req.URL.Path,
			"time":   time.Since(start),
		}).Info("Started a request")

		c.Next()
		rw := res.(martini.ResponseWriter)

		log.WithFields(logrus.Fields{
			"method": req.Method,
			"path":   req.URL.Path,
			"status": rw.Status(),
			"time":   time.Since(start),
		}).Info("Completed a request")
	}
}

func dynamiqMartini() *martini.ClassicMartini {
	r := martini.NewRouter()
	m := martini.New()

	m.Map(logrus.New())
	m.Use(logrusLogger())
	m.Use(martini.Recovery())
	m.Use(martini.Static("public"))
	m.MapTo(r, (*martini.Routes)(nil))
	m.Action(r.Handle)
	return &martini.ClassicMartini{m, r}
}

type HTTP_API_V1 struct {
}

func (h HTTP_API_V1) InitWebserver(list *memberlist.Memberlist, cfg *Config) {
	// tieing our Queue to HTTP interface == bad we should move this somewhere else
	// Queues.Queues is dumb. Need a better name-chain
	queues := cfg.Queues
	// also tieing topics this is next for refactor
	topics := InitTopics(cfg, queues)
	m := dynamiqMartini()
	m.Use(render.Renderer())

	// Group the routes underneath their version
	m.Group("/v1", func(r martini.Router) {
		// STATUS / STATISTICS API BLOCK
		m.Get("/status/servers", func() string {
			return_string := ""
			for _, member := range list.Members() {
				return_string += fmt.Sprintf("Member: %s %s\n", member.Name, member.Addr)
			}
			return return_string
		})
		// END STATUS / STATISTICS API BLOCK

		// CONFIGURATION API BLOCK

		m.Delete("/topics/:topic", func(r render.Render, params martini.Params) {
			var present bool
			_, present = topics.TopicMap[params["topic"]]
			if present == true {
				deleted := topics.DeleteTopic(params["topic"])
				r.JSON(200, map[string]interface{}{"Deleted": deleted})
			} else {
				r.JSON(404, map[string]interface{}{"error": "Topic did not exist."})
			}
		})

		m.Put("/queues/:queue", func(r render.Render, params martini.Params) {
			var present bool
			_, present = queues.QueueMap[params["queue"]]
			if present != true {
				cfg.InitializeQueue(params["queue"])
				r.JSON(201, "created")
			} else {
				r.JSON(422, map[string]interface{}{"error": "Queue already exists."})
			}
		})

		m.Put("/topics/:topic", func(r render.Render, params martini.Params) {
			var present bool
			_, present = topics.TopicMap[params["topic"]]
			if present != true {
				topics.InitTopic(params["topic"])
				r.JSON(201, map[string]interface{}{"Queues": topics.TopicMap[params["topic"]].ListQueues()})
			} else {
				r.JSON(422, map[string]interface{}{"error": "Topic already exists."})
			}
		})

		m.Put("/topics/:topic/queues/:queue", func(r render.Render, params martini.Params) {
			var present bool
			_, present = topics.TopicMap[params["topic"]]
			if present != true {
				r.JSON(422, map[string]interface{}{"error": "Topic does not exist. Please create it first."})
			} else {
				_, present = queues.QueueMap[params["queue"]]
				if present != true {
					r.JSON(422, map[string]interface{}{"error": "Queue does not exist. Please create it first"})
				} else {
					topics.TopicMap[params["topic"]].AddQueue(cfg, params["queue"])
					r.JSON(200, map[string]interface{}{"Queues": topics.TopicMap[params["topic"]].ListQueues()})
				}
			}
		})

		// neeeds a little work....
		m.Delete("/topics/:topic/queues/:queue", func(r render.Render, params martini.Params) {
			var present bool
			_, present = topics.TopicMap[params["topic"]]
			if present != true {
				topics.InitTopic(params["topic"])
			}
			topics.TopicMap[params["topic"]].DeleteQueue(cfg, params["queue"])
			r.JSON(200, map[string]interface{}{"Queues": topics.TopicMap[params["topic"]].ListQueues()})
		})

		m.Patch("/queues/:queue", binding.Json(ConfigRequest{}), func(configRequest ConfigRequest, r render.Render, params martini.Params) {
			// Not sure of better way to get the queue name from the request, but would be good to not
			// Have to reach into params - better to bind it

			// There is probably an optimal order to set these in, depending on if the values for min/max
			// have shrunk or grown, so the system can self-adjust in the correct fashion. Or, maybe it's fine to do it however

			// Should we detect and throw errors on incorrect configuration names? Would make debugging simpler

			// Likely all of this belongs in config, where we just pass in an interface to the request object, and it
			// returns the first error it runs across. Would simplify the code here greatly.
			var err error
			if configRequest.VisibilityTimeout != nil {
				err = cfg.SetVisibilityTimeout(params["queue"], *configRequest.VisibilityTimeout)
				// We really need a proper way to generalize error handling
				// Writing this out every time is going to be silly
				if err != nil {
					logrus.Println(err)
					r.JSON(500, map[string]interface{}{"error": err.Error()})
					return
				}
			}

			if configRequest.MinPartitions != nil {
				err = cfg.SetMinPartitions(params["queue"], *configRequest.MinPartitions)
				if err != nil {
					logrus.Println(err)
					r.JSON(500, map[string]interface{}{"error": err.Error()})
					return
				}
			}

			if configRequest.MaxPartitions != nil {
				err = cfg.SetMaxPartitions(params["queue"], *configRequest.MaxPartitions)
				if err != nil {
					logrus.Println(err)
					r.JSON(500, map[string]interface{}{"error": err.Error()})
					return
				}
			}
			if configRequest.MaxPartitionAge != nil {
				err = cfg.SetMaxPartitionAge(params["queue"], *configRequest.MaxPartitionAge)
				if err != nil {
					logrus.Println(err)
					r.JSON(500, map[string]interface{}{"error": err.Error()})
					return
				}
			}

			r.JSON(200, "ok")
		})

		// END CONFIGURATION API BLOCK

		// DATA INTERACTION API BLOCK

		m.Get("/topics", func(r render.Render) {
			topicList := make([]string, 0, 10)
			for topicName, _ := range topics.TopicMap {
				topicList = append(topicList, topicName)
			}
			r.JSON(200, map[string]interface{}{"topics": topicList})
		})

		m.Get("/topics/:topic", func(r render.Render, params martini.Params) {
			var present bool
			_, present = topics.TopicMap[params["topic"]]
			if present != true {
				topics.InitTopic(params["topic"])
			}

			r.JSON(200, map[string]interface{}{"Queues": topics.TopicMap[params["topic"]].ListQueues()})
		})

		m.Put("/topics/:topic/message", func(r render.Render, params martini.Params, req *http.Request) {
			var present bool
			_, present = topics.TopicMap[params["topic"]]
			if present != true {
				topics.InitTopic(params["topic"])
			}
			var buf bytes.Buffer
			buf.ReadFrom(req.Body)

			response := topics.TopicMap[params["topic"]].Broadcast(cfg, buf.String())
			r.JSON(200, response)
		})

		m.Get("/queues", func(r render.Render, params martini.Params) {
			queueList := make([]string, 0, 10)
			for queueName, _ := range queues.QueueMap {
				queueList = append(queueList, queueName)
			}
			r.JSON(200, map[string]interface{}{"queues": queueList})
		})

		m.Get("/queues/:queue", func(r render.Render, params martini.Params) {
			//check if we've initialized this queue yet
			var present bool
			_, present = queues.QueueMap[params["queue"]]
			if present == true {
				queueReturn := make(map[string]interface{})
				queueReturn["visibility_timeout"], _ = cfg.GetVisibilityTimeout(params["queue"])
				queueReturn["min_partitions"], _ = cfg.GetMinPartitions(params["queue"])
				queueReturn["max_partitions"], _ = cfg.GetMaxPartitions(params["queue"])
				queueReturn["max_partition_age"], _ = cfg.GetMaxPartitionAge(params["queue"])
				queueReturn["partitions"] = queues.QueueMap[params["queue"]].Parts.PartitionCount()
				r.JSON(200, queueReturn)
			} else {
				r.JSON(404, fmt.Sprintf("There is no queue named %s", params["queue"]))
			}
		})

		m.Get("/queues/:queue/messages/:batchSize", func(r render.Render, params martini.Params) {
			//check if we've initialized this queue yet
			var present bool
			_, present = queues.QueueMap[params["queue"]]
			if present == true {
				batchSize, err := strconv.ParseInt(params["batchSize"], 10, 64)
				if err != nil {
					//log the error for unparsable input
					logrus.Error(err)
					r.JSON(422, err.Error())
				}
				if batchSize <= 0 {
					r.JSON(422, fmt.Sprint("Batchsizes must be non-negative integers greater than 0"))
				}
				messages, err := queues.QueueMap[params["queue"]].Get(cfg, list, batchSize)

				if err != nil && err.Error() != NOPARTITIONS {
					r.JSON(204, err.Error())
				}
				//TODO move this into the Queue.Get code
				messageList := make([]map[string]interface{}, 0, 10)
				//Format response
				for _, object := range messages {
					message := make(map[string]interface{})
					message["id"] = object.Key
					message["body"] = string(object.Data[:])
					messageList = append(messageList, message)
				}
				if err != nil && err.Error() != NOPARTITIONS {
					logrus.Error(err)
					r.JSON(500, err.Error())
				} else {
					r.JSON(200, messageList)
				}
			} else {
				// What is a sane result here?
				r.JSON(404, fmt.Sprintf("There is no queue named %s", params["queue"]))
			}
		})

		m.Put("/queues/:queue/message", func(params martini.Params, req *http.Request) string {
			var present bool
			_, present = queues.QueueMap[params["queue"]]
			if present == true {
				// parse the request body into a sting
				// TODO clean this up, full json api?
				var buf bytes.Buffer
				buf.ReadFrom(req.Body)
				uuid := queues.QueueMap[params["queue"]].Put(cfg, buf.String())

				return uuid
			} else {
				// What is a sane response to this?
				return ""
			}
		})

		m.Delete("/queues/:queue/message/:messageId", func(r render.Render, params martini.Params) {
			var present bool
			_, present = queues.QueueMap[params["queue"]]
			if present != true {
				cfg.InitializeQueue(params["queue"])
			}

			r.JSON(200, queues.QueueMap[params["queue"]].Delete(cfg, params["messageId"]))
		})
		// DATA INTERACTION API BLOCK
	})
	logrus.Fatal(http.ListenAndServe(":"+strconv.Itoa(cfg.Core.HttpPort), m))
}
