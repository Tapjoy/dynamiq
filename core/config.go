package core

import (
	"errors"
	"log"
	"sync"
	"time"

	"github.com/Tapjoy/dynamiq/app/compressor"
	"github.com/Tapjoy/dynamiq/app/stats"
	"github.com/hashicorp/memberlist"
)

// StatsConfig represents config info for sending data to any statsd like system
// and the client itself
type StatsConfig struct {
	Address       string
	FlushInterval int
	Prefix        string
	Client        *stats.Client
}

// HTTPConfig represents config info for the HTTP server
type HTTPConfig struct {
	APIVersion string
	Port       uint16
}

// DiscoveryConfig represents config info for how Dynamiq nodes discovery eachother via Memberlist
type DiscoveryConfig struct {
	Port       int
	Memberlist *memberlist.Memberlist
}

// RiakConfig represents config info and the connection pool for Riak
type RiakConfig struct {
	Addresses          []string
	Port               uint16
	Service            *RiakService
	ConfigSyncInterval time.Duration
}

// Config is the parent struct of all the individual configuration sections
type Config struct {
	Riak       *RiakConfig
	Discovery  *DiscoveryConfig
	HTTP       *HTTPConfig
	Stats      *StatsConfig
	Compressor *compressor.Compressor
	Queues     *Queues
	Topics     *Topics
	// Channels / Timer for syncing the config
	syncScheduler *time.Ticker
	syncKiller    chan bool
}

var (
	//ErrUnknownTopic is
	ErrUnknownTopic = errors.New("There is no known topic by that name")
)

// GetConfig Parses and returns a config object
func GetConfig() (*Config, error) {
	// TODO settle on an actual config package

	discoveryConfig := &DiscoveryConfig{
		Port: 7000,
	}

	httpConfig := &HTTPConfig{
		Port:       8081,
		APIVersion: "2",
	}

	riakConfig := &RiakConfig{
		Addresses:          []string{"127.0.0.1"},
		Port:               8087,
		ConfigSyncInterval: 2 * time.Second,
	}

	rs, err := NewRiakService(riakConfig.Addresses, riakConfig.Port)
	if err != nil {
		return nil, err
	}
	riakConfig.Service = rs

	t, err := LoadTopicsFromRiak(riakConfig)
	if err != nil {
		return nil, err
	}
	q, err := LoadQueuesFromRiak(riakConfig)
	if err != nil {
		return nil, err
	}

	cfg := &Config{
		Riak:          riakConfig,
		Discovery:     discoveryConfig,
		HTTP:          httpConfig,
		Queues:        q,
		Topics:        t,
		syncScheduler: time.NewTicker(riakConfig.ConfigSyncInterval),
		syncKiller:    make(chan bool),
	}
	cfg.beginSync()
	return cfg, nil
}

// TopicNames is
func (cfg *Config) TopicNames() []string {
	cfg.Topics.configLock.RLock()
	list := make([]string, 0)

	for name := range cfg.Topics.KnownTopics {
		list = append(list, name)
	}
	cfg.Topics.configLock.RUnlock()
	return list
}

// GetTopic is
func (cfg *Config) GetTopic(name string) (*Topic, error) {
	cfg.Topics.configLock.RLock()
	t, ok := cfg.Topics.KnownTopics[name]
	cfg.Topics.configLock.RUnlock()
	if ok {
		return t, nil
	}
	return nil, ErrUnknownTopic
}

func (cfg *Config) beginSync() {
	// Go routine to listen to either the scheduler or the killer
	go func(config *Config) {
		for {
			select {
			// Check to see if we have a tick
			case <-config.syncScheduler.C:
				config.syncConfig()
				// Check to see if we've been stopped
			case <-config.syncKiller:
				config.syncScheduler.Stop()
				return
			}
		}
	}(cfg)
	return
}

func (cfg *Config) syncConfig() error {
	log.Println("Syncing")
	// First - Refresh the list of topics and their metadata
	tcfg, err := cfg.Riak.Service.GetTopicsConfigMap()
	if err != nil {
		if err == ErrConfigMapNotFound {
			tcfg, err = cfg.Riak.Service.CreateTopicsConfigMap()
			if err != nil {
				return err
			}
		}
		return err
	}
	var oldSet [][]byte
	var ok bool
	cfg.Topics.configLock.Lock()
	defer cfg.Topics.configLock.Unlock()

	if oldSet, ok = tcfg.Sets["topics"]; !ok {
		// There were no known topics ?
		// No known topics should not prevent the entire sync
		// just the topic portion
		log.Println(ErrNoKnownTopics)
		return ErrNoKnownTopics
	}

	if len(oldSet) == 0 {
		log.Println(ErrNoKnownTopics)

		return ErrNoKnownTopics
	}

	cfg.Topics.Config = tcfg
	var newSet [][]byte
	if newSet, ok = cfg.Topics.Config.Sets["topics"]; !ok {
		log.Println(ErrNoKnownTopics)

		return ErrNoKnownTopics
	}

	// If a topic exists in topicsToKeep...
	// ... and does exist in Topics, update it's config Object
	// ... and does not exist in Topics, initialize it from Riak
	// Else
	// ... It is not in toKeep and is in Topics, remove it

	topicsToKeep := make(map[string]bool)
	for _, topic := range newSet {
		tName := string(topic)
		// Record this so we know who to evict
		topicsToKeep[tName] = true
		// Add or Update the topic to the known set
		var t *Topic
		if t, ok = cfg.Topics.KnownTopics[tName]; !ok {
			// Didn't exist in memory, so create it
			// TODO centralize this, don't just re-write initialization logic
			t = &Topic{
				Name:       tName,
				configLock: sync.RWMutex{},
			}
		}
		// get the config and set it on the topic
		topcfg, err := cfg.Riak.Service.GetTopicConfigMap(tName)
		if err != nil {
			// If the config object isn't found - no big deal
			// Only config presently is a list of queues, the object will
			// be created when the first queue is set
			log.Println(err, tName)
		}
		t.Config = topcfg
		cfg.Topics.KnownTopics[tName] = t
	}

	// Go through the old list of topics
	for _, topic := range oldSet {
		tName := string(topic)
		if _, ok := topicsToKeep[tName]; !ok {
			// It wasn't in the old list, evict it
			delete(cfg.Topics.KnownTopics, tName)
		}
	}

	// Now, do the above but for the queues. Added effect - any queue removed
	// must also be removed from a topic

	qcfg, err := cfg.Riak.Service.GetQueuesConfigMap()
	if err != nil {
		if err != nil {
			if err == ErrConfigMapNotFound {
				qcfg, err = cfg.Riak.Service.CreateQueuesConfigMap()
				if err != nil {
					return err
				}
			}
			return err
		}
	}

	cfg.Queues.configLock.Lock()
	defer cfg.Queues.configLock.Unlock()
	if oldSet, ok = qcfg.Sets["queues"]; !ok {
		// There were no known topics ?
		// No known topics should not prevent the entire sync
		// just the topic portion
		log.Println(ErrNoKnownQueues)
		return ErrNoKnownQueues
	}

	if len(oldSet) == 0 {
		log.Println(ErrNoKnownQueues)
		return ErrNoKnownQueues
	}

	cfg.Queues.Config = qcfg
	if newSet, ok = cfg.Queues.Config.Sets["queues"]; !ok {
		log.Println(ErrNoKnownQueues)
		return ErrNoKnownQueues
	}

	queuesToRemove := make([]string, 0)
	queuesToKeep := make(map[string]bool)

	for _, queue := range newSet {
		qName := string(queue)
		// Record this so we know who to evict
		topicsToKeep[qName] = true
		// Add or Update the topic to the known set
		var q *Queue
		if q, ok = cfg.Queues.KnownQueues[qName]; !ok {
			// Didn't exist in memory, so create it
			// TODO centralize this, don't just re-write initialization logic
			q = &Queue{
				Name:       qName,
				configLock: sync.RWMutex{},
			}
		}
		// get the config and set it on the topic
		queuecfg, err := cfg.Riak.Service.GetQueueConfigMap(qName)
		if err != nil {
			log.Println(err, qName)
			return err
		}
		q.Config = queuecfg
		cfg.Queues.KnownQueues[qName] = q
	}

	// Go through the old list of queues
	for _, queue := range oldSet {
		qName := string(queue)
		if _, ok := queuesToKeep[qName]; !ok {
			// It wasn't in the old list, evict it
			delete(cfg.Queues.KnownQueues, qName)
		} else {
			queuesToRemove = append(queuesToRemove, qName)
		}
	}

	// Go through any evicted queues, and remove them from topic subscriptions
	// 3 loops... there has to be a better way to manage all of this
	for _, queue := range queuesToRemove {
		for tName, topic := range cfg.Topics.KnownTopics {
			for _, q := range topic.Config.Sets["queues"] {
				qName := string(q)
				if qName == queue {
					_, err := cfg.Riak.Service.UpdateTopicSubscription(tName, qName, false)
					if err != nil {
						log.Println(err)
						return err
					}
				}
			}
		}
	}
	log.Println("Syncing Done")
	return nil
}
