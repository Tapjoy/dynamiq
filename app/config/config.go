package config

import (
	"code.google.com/p/gcfg"
	//"encoding/json"
	//"github.com/hashicorp/memberlist"
	"errors"
	"github.com/tpjg/goriakpbc"
	"log"
	"strconv"
	"time"
)

var (
	ConfigurationOptionNotFound = errors.New("Configuration Value Not Found")
)

const CONFIGURATION_BUCKET = "config"
const SET_BUCKET = "config_sets"
const QUEUE_SET_NAME = "queues_list"
const TOPIC_SET_NAME = "topic_list"

const VISIBILITY_TIMEOUT = "visibility_timeout"
const PARTITION_COUNT = "partition_count"
const MIN_PARTITIONS = "min_partitions"
const MAX_PARTITIONS = "max_partitions"

// Arrays and maps cannot be made immutable in golang
var SETTINGS = [...]string{VISIBILITY_TIMEOUT, PARTITION_COUNT, MIN_PARTITIONS, MAX_PARTITIONS}
var DEFAULT_SETTINGS = map[string]string{VISIBILITY_TIMEOUT: "30", PARTITION_COUNT: "50", MIN_PARTITIONS: "10", MAX_PARTITIONS: "100"}

type Config struct {
	Core   Core
	queues Queues
}

type Core struct {
	Name                  string
	Port                  int
	SeedServer            string
	SeedPort              int
	HttpPort              int
	Visibility            float64
	RiakNodes             string
	BackendConnectionPool int
	InitPartitions        int
	MaxPartitions         int
	SyncConfigInterval    time.Duration
}

type Queues struct {
	settings map[string]map[string]string
	riakPool RiakPool
}

func GetCoreConfig(config_file *string) (Config, error) {
	var cfg Config
	err := gcfg.ReadFileInto(&cfg, *config_file)
	if err != nil {
		log.Fatal(err)
	}
	pool := InitRiakPool(cfg)
	cfg.queues = loadQueuesConfig(pool)
	return cfg, err
}

func (cfg Config) InitializeQueue(queueName string) error {
	// Create the configuration data in Riak first
	// This way it'll be there once the queue is added to the known set
	err := cfg.queues.createConfigForQueue(queueName)
	if err != nil {
		return err
	}
	// Add to the known set of queues
	err = cfg.queues.addToKnownQueues(queueName)
	return err
}

func (queues Queues) addToKnownQueues(queueName string) error {
	client := queues.riakConnection()
	bucket, _ := client.NewBucketType("sets", SET_BUCKET)
	queueSet, _ := bucket.FetchSet(QUEUE_SET_NAME)
	queueSet.Add([]byte(queueName))
	return queueSet.Store()
}

func (queues Queues) RemoveFromKnownQueues(queueName string) error {
	client := queues.riakConnection()
	bucket, _ := client.NewBucket(CONFIGURATION_BUCKET)
	queueSet, _ := bucket.FetchSet(QUEUE_SET_NAME)
	queueSet.Remove([]byte(queueName))
	return queueSet.Store()
}

func (cfg Config) GetQueueSettings(queueName string) map[string]string {
	return cfg.queues.settings[queueName]
}

func loadQueuesConfig(riakPool RiakPool) Queues {
	// Create the Queues Config struct
	queueConfig := Queues{
		// RiakPool does not belong here - it should be moved upto Config
		riakPool: riakPool,
		settings: make(map[string]map[string]string),
	}
	// Get the queues
	client := riakPool.GetConn()
	// TODO: We should be handling errors here
	// Get the bucket holding the sets of config data
	bucket, _ := client.NewBucketType("sets", SET_BUCKET)
	// Fetch the object for holding the set of queues
	queueSet, _ := bucket.FetchSet(QUEUE_SET_NAME)
	// For each queue we have in the system
	for _, elem := range queueSet.GetValue() {
		// Convert it's name into a string
		name := string(elem[:])
		// Pre-warm the settings object
		queueConfig.settings[name] = make(map[string]string)
		// Get the bucket for holding maps of config data
		configBucket, _ := client.NewBucketType("maps", CONFIGURATION_BUCKET)
		// TODO: We should be handling errors here
		// Get the Map of values for that queue
		obj, _ := configBucket.FetchMap(name)
		// For each known setting name
		for _, setting := range SETTINGS {
			// Get the value as a string
			strVal := registerValueToString(obj.FetchRegister(setting))
			// Set in cache
			queueConfig.settings[name][setting] = strVal
		}
	}
	// Return the completed Queue cache of settings
	return queueConfig
}

// TODO: Take in a map which overrides the defaults
func (queues Queues) createConfigForQueue(queueName string) error {
	client := queues.riakConnection()
	// Get the bucket for holding maps of config data
	// TODO: Find a nice way to DRY this up - it's a lil copy/pasty
	bucket, _ := client.NewBucketType("maps", CONFIGURATION_BUCKET)
	// Get the object for this queues settings
	obj, _ := bucket.FetchMap(queueName)
	// For each known setting
	for _, elem := range SETTINGS {
		// Get the reigster for this setting
		reg := obj.AddRegister(elem)
		// Convert the default value to a bytearray, set it on the Register
		reg.Update([]byte(DEFAULT_SETTINGS[elem]))
	}
	// Save the object, returns an error up the callchain if needed
	return obj.Store()
}

func (cfg Config) GetVisibilityTimeout(queueName string) (int, error) {
	val, err := cfg.queues.get(VISIBILITY_TIMEOUT, queueName)
	parsed, err := strconv.Atoi(val)
	return parsed, err
}

func (cfg Config) SetVisibilityTimeout(queueName string, timeout int) error {
	return cfg.queues.set(VISIBILITY_TIMEOUT, queueName, strconv.Itoa(timeout))
}

func (cfg Config) GetMinPartitions(queueName string) (int, error) {
	val, err := cfg.queues.get(MIN_PARTITIONS, queueName)
	parsed, err := strconv.Atoi(val)
	return parsed, err
}

func (cfg Config) SetMinPartitions(queueName string, timeout int) error {
	// TODO do we handle any resizing here? Or does the system "self-adjust"
	return cfg.queues.set(MIN_PARTITIONS, queueName, strconv.Itoa(timeout))
}

func (cfg Config) GetMaxPartitions(queueName string) (int, error) {
	val, err := cfg.queues.get(MAX_PARTITIONS, queueName)
	parsed, err := strconv.Atoi(val)
	return parsed, err
}

func (cfg Config) SetMaxPartitions(queueName string, timeout int) error {
	// TODO do we handle any resizing here? Or does the system "self-adjust"
	return cfg.queues.set(MAX_PARTITIONS, queueName, strconv.Itoa(timeout))
}

// TODO Is this even wise? Do we want to store this in config info?
// It should probably be better determined by probing the system dynamically...
func (cfg Config) GetPartitionCount(queueName string) (int, error) {
	val, err := cfg.queues.get(PARTITION_COUNT, queueName)
	parsed, err := strconv.Atoi(val)
	return parsed, err
}

func (queues Queues) get(paramName string, queueName string) (string, error) {
	// Read from local cache
	value := queues.settings[queueName][paramName]
	var err error
	if value == "" {
		// Read from riak
		client := queues.riakConnection()
		bucket, _ := client.NewBucketType("maps", CONFIGURATION_BUCKET)
		obj, err := bucket.FetchMap(queueName)

		// if not found... no config existed for that queue - should not happen hashtagcrossfingers
		if err == riak.NotFound {
			// Log out an error here
			return "", err
		}

		val := obj.FetchRegister(paramName)

		if val != nil {
			// We had a register with this name, return the value
			value = registerValueToString(val)
		}
	}
	return value, err
}

func (queues Queues) set(paramName string, queueName string, value string) error {
	// Set the local cache
	queues.settings[queueName][paramName] = value
	// Write to Riak
	client := queues.riakConnection()
	bucket, _ := client.NewBucketType("maps", CONFIGURATION_BUCKET)
	obj, err := bucket.FetchMap(queueName)
	// if not found... no config existed for that queue - should not happen hashtagcrossfingers
	if err == riak.NotFound {
		// Log out an error here
		return err
	}
	val := obj.AddRegister(paramName)
	val.NewValue = []byte(value)
	// Communicate to memberlist
	return obj.Store()
}

func registerValueToString(reg *riak.RDtRegister) string {
	return string(reg.Value[:])
}

func (queues Queues) riakConnection() *riak.Client {
	conn := queues.riakPool.GetConn()
	return conn
}
