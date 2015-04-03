package app

import (
	"code.google.com/p/gcfg"
	"errors"
	"fmt"
	"github.com/Sirupsen/logrus"
	"github.com/Tapjoy/dynamiq/app/compressor"
	"github.com/Tapjoy/dynamiq/app/stats"
	"github.com/tpjg/goriakpbc"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

var (
	ConfigurationOptionNotFound = errors.New("Configuration Value Not Found")
)

const CONFIGURATION_BUCKET = "config"
const QUEUE_CONFIG_NAME = "queue_config"
const QUEUE_SET_NAME = "queues"

const VISIBILITY_TIMEOUT = "visibility_timeout"
const PARTITION_COUNT = "partition_count"
const MIN_PARTITIONS = "min_partitions"
const MAX_PARTITIONS = "max_partitions"
const MAX_PARTITION_AGE = "max_partition_age"
const COMPRESSED_MESSAGES = "compressed_messages"

// Arrays and maps cannot be made immutable in golang
var SETTINGS = [...]string{VISIBILITY_TIMEOUT, PARTITION_COUNT, MIN_PARTITIONS, MAX_PARTITIONS, MAX_PARTITION_AGE, COMPRESSED_MESSAGES}
var DEFAULT_SETTINGS = map[string]string{VISIBILITY_TIMEOUT: "30", PARTITION_COUNT: "5", MIN_PARTITIONS: "1", MAX_PARTITIONS: "10", MAX_PARTITION_AGE: "432000", COMPRESSED_MESSAGES: "false"}

type Config struct {
	Core       Core
	Stats      Stats
	Compressor compressor.Compressor
	Queues     *Queues
	RiakPool   *riak.Client
	Topics     *Topics
}

type Core struct {
	Name                  string
	Port                  int
	SeedServer            string
	SeedPort              int
	SeedServers           []string
	HttpPort              int
	RiakNodes             string
	BackendConnectionPool int
	SyncConfigInterval    time.Duration
	LogLevel              logrus.Level
	LogLevelString        string
}

type Stats struct {
	Type                 string
	FlushInterval        int
	Address              string
	Prefix               string
	Client               stats.StatsClient
	InternalStatsdServer bool
}

func initRiakPool(cfg *Config) *riak.Client {
	rand.Seed(time.Now().UnixNano())
	// TODO this should just be 1 HAProxy
	hosts := []string{cfg.Core.RiakNodes}
	host := hosts[rand.Intn(len(hosts))]
	return riak.NewClientPool(host, cfg.Core.BackendConnectionPool)
}

func GetCoreConfig(config_file *string) (*Config, error) {
	var cfg Config
	err := gcfg.ReadFileInto(&cfg, *config_file)
	if err != nil {
		logrus.Fatal(err)
	}

	if len(cfg.Core.SeedServer) == 0 {
		logrus.Fatal("The list of seedservers was empty")
	}

	cfg.Core.SeedServers = strings.Split(cfg.Core.SeedServer, ",")
	for i, x := range cfg.Core.SeedServers {
		cfg.Core.SeedServers[i] = x + ":" + strconv.Itoa(cfg.Core.SeedPort)
	}

	cfg.RiakPool = initRiakPool(&cfg)
	cfg.Queues = loadQueuesConfig(&cfg)
	switch cfg.Stats.Type {
	case "statsd":
		cfg.Stats.Client = stats.NewStatsdClient(cfg.Stats.Address, cfg.Stats.Prefix, time.Second*time.Duration(cfg.Stats.FlushInterval))
	default:
		cfg.Stats.Client = stats.NewNOOPClient()
	}

	// Currently we only support zlib, but we may support others
	// Here is where we'd detect and inject
	cfg.Compressor = compressor.NewZlibCompressor()

	cfg.Core.LogLevel, err = logrus.ParseLevel(cfg.Core.LogLevelString)
	if err != nil {
		logrus.Fatal(err)
	}

	go cfg.Queues.syncConfig(&cfg)
	return &cfg, err
}

func loadQueuesConfig(cfg *Config) *Queues {
	// Create the Queues Config struct
	queuesConfig := Queues{
		QueueMap: make(map[string]*Queue),
	}
	// Get the queues
	client := cfg.RiakConnection()
	// TODO: We should be handling errors here
	// Get the bucket holding the map of config data
	configBucket, _ := client.NewBucketType("maps", CONFIGURATION_BUCKET)
	// Fetch the object for holding the set of queues
	config, _ := configBucket.FetchMap(QUEUE_CONFIG_NAME)
	queuesConfig.Config = config

	// AddSet implicitly calls fetch set if the set already exists
	queueSet := config.AddSet(QUEUE_SET_NAME)
	if queueSet == nil {
		queueSet.Add([]byte("default_queue"))
		config.Store()
		config, _ = configBucket.FetchMap(QUEUE_CONFIG_NAME)
	}
	// For each queue we have in the system
	for _, elem := range queueSet.GetValue() {
		// Convert it's name into a string
		name := string(elem[:])
		// Get the Riak RdtMap of settings for this queue
		configMap, _ := configBucket.FetchMap(queueConfigRecordName(name))
		// Pre-warm the settings object
		queue := &Queue{
			Name:   name,
			Config: configMap,
			Parts:  InitPartitions(cfg, name),
		}
		// TODO: We should be handling errors here
		// Set the queue in the queue map
		queuesConfig.QueueMap[name] = queue
	}
	// Return the completed Queue cache of settings
	return &queuesConfig
}

func (cfg *Config) InitializeQueue(queueName string) error {
	// Create the configuration data in Riak first
	// This way it'll be there once the queue is added to the known set
	configMap, err := cfg.createConfigForQueue(queueName)
	if err != nil {
		return err
	}
	// Add to the known set of queues
	err = cfg.addToKnownQueues(queueName)
	// Now, add the queue into our memory-cache of data
	cfg.Queues.QueueMap[queueName] = &Queue{
		Name:   queueName,
		Parts:  InitPartitions(cfg, queueName),
		Config: configMap,
	}
	return err
}

func (cfg *Config) addToKnownQueues(queueName string) error {
	// If we disallow topicless-queues, we can remove this and put it into Topic.AddQueue
	client := cfg.RiakConnection()
	// We purposefully read from Riak here, we'll enventually-consist with the in memory cache
	bucket, _ := client.NewBucketType("maps", CONFIGURATION_BUCKET)
	queueConfig, _ := bucket.FetchMap(QUEUE_CONFIG_NAME)
	queueSet := queueConfig.AddSet(QUEUE_SET_NAME)
	queueSet.Add([]byte(queueName))
	return queueConfig.Store()
}

func (cfg *Config) removeFromKnownQueues(queueName string) error {
	// If we disallow topicless-queues, we can remove this and put it into Topic.RemoveQueue
	client := cfg.RiakConnection()
	// We purposefully read from Riak here, we'll enventually-consist with the in memory cache
	bucket, _ := client.NewBucketType("maps", CONFIGURATION_BUCKET)
	queueConfig, _ := bucket.FetchMap(QUEUE_CONFIG_NAME)
	queueSet := queueConfig.AddSet(QUEUE_SET_NAME)
	queueSet.Remove([]byte(queueName))
	return queueConfig.Store()
}

// TODO: Take in a map which overrides the defaults
func (cfg *Config) createConfigForQueue(queueName string) (*riak.RDtMap, error) {
	client := cfg.RiakConnection()
	// Get the bucket for holding maps of config data
	// TODO: Find a nice way to DRY this up - it's a lil copy/pasty
	bucket, _ := client.NewBucketType("maps", CONFIGURATION_BUCKET)
	// Get the object for this queues settings
	obj, _ := bucket.FetchMap(queueConfigRecordName(queueName))
	// For each known setting
	for _, elem := range SETTINGS {
		// Get the reigster for this setting
		reg := obj.AddRegister(elem)
		// Convert the default value to a bytearray, set it on the Register
		reg.Update([]byte(DEFAULT_SETTINGS[elem]))
	}
	// Save the object, returns an error up the callchain if needed
	return obj, obj.Store()
}

// SETTERS AND GETTERS FOR QUEUE CONFIG

func (cfg *Config) GetVisibilityTimeout(queueName string) (float64, error) {
	val, err := cfg.getQueueSetting(VISIBILITY_TIMEOUT, queueName)
	parsed, err := strconv.ParseFloat(val, 32)
	return parsed, err
}

func (cfg *Config) SetVisibilityTimeout(queueName string, timeout float64) error {
	return cfg.setQueueSetting(VISIBILITY_TIMEOUT, queueName, strconv.FormatFloat(timeout, 'f', -1, 64))
}

func (cfg *Config) GetMinPartitions(queueName string) (int, error) {
	val, _ := cfg.getQueueSetting(MIN_PARTITIONS, queueName)
	return strconv.Atoi(val)
}

func (cfg *Config) SetMinPartitions(queueName string, timeout int) error {
	// TODO do we handle any resizing here? Or does the system "self-adjust"
	return cfg.setQueueSetting(MIN_PARTITIONS, queueName, strconv.Itoa(timeout))
}

func (cfg *Config) GetMaxPartitions(queueName string) (int, error) {
	val, _ := cfg.getQueueSetting(MAX_PARTITIONS, queueName)
	return strconv.Atoi(val)
}

func (cfg *Config) SetMaxPartitions(queueName string, timeout int) error {
	// TODO do we handle any resizing here? Or does the system "self-adjust"
	return cfg.setQueueSetting(MAX_PARTITIONS, queueName, strconv.Itoa(timeout))
}
func (cfg *Config) SetMaxPartitionAge(queueName string, age float64) error {
	return cfg.setQueueSetting(MAX_PARTITION_AGE, queueName, strconv.FormatFloat(age, 'f', -1, 64))
}
func (cfg *Config) GetMaxPartitionAge(queueName string) (float64, error) {
	val, _ := cfg.getQueueSetting(MAX_PARTITION_AGE, queueName)
	return strconv.ParseFloat(val, 32)
}

func (cfg *Config) GetCompressedMessages(queueName string) (bool, error) {
	val, _ := cfg.getQueueSetting(COMPRESSED_MESSAGES, queueName)
	return strconv.ParseBool(val)
}

func (cfg *Config) SetCompressedMessages(queueName string, compressedMessages bool) error {
	return cfg.setQueueSetting(COMPRESSED_MESSAGES, queueName, strconv.FormatBool(compressedMessages))
}

// TODO Find a proper way to scope this to a queue VS a topic
func (cfg *Config) getQueueSetting(paramName string, queueName string) (string, error) {
	// Read from local cache
	value := ""
	var err error
	// If cfg.Queues is nil, it means we're in the middle of booting, and we're trying to configure
	// partition counts. If this is the case, skip to reading directly from Riak
	// This means booting up will incur a number of extra reads to Riak
	// We can likely improve this, but it works for the time being

	// If cfg.Queues.QueueMap[queuename] is nil, it means this server hasn't yet synced with Riak
	// While we wait, go and read from Riak directly
	if cfg.Queues != nil {
		if _, ok := cfg.Queues.QueueMap[queueName]; ok {
			regValue := cfg.Queues.QueueMap[queueName].getQueueConfig().FetchRegister(paramName)
			if regValue != nil {
				value, err = registerValueToString(regValue)
				if err != nil {
					return value, err
				}
			} else {
				// There is a chance the queue pre-dated the existence of the given parameter. If so, use the
				// configured default value for now
				// TODO - Need to backfill missing params when they're detected
				value = DEFAULT_SETTINGS[paramName]
			}
		}
	}

	if value == "" {
		// Read from riak
		client := cfg.RiakConnection()
		bucket, _ := client.NewBucketType("maps", CONFIGURATION_BUCKET)
		obj, err := bucket.FetchMap(queueConfigRecordName(queueName))

		// if not found... no config existed for that queue - should not happen hashtagcrossfingers
		if err == riak.NotFound {
			// Log out an error here
			return "", err
		}

		val := obj.FetchRegister(paramName)

		if val != nil {
			// We had a register with this name, return the value
			value, err = registerValueToString(val)
		}
	}
	return value, err
}

// TODO Find a proper way to scope this to a queue VS a topic
func (cfg *Config) setQueueSetting(paramName string, queueName string, value string) error {
	// Write to Riak
	client := cfg.RiakConnection()
	bucket, _ := client.NewBucketType("maps", CONFIGURATION_BUCKET)
	obj, err := bucket.FetchMap(queueConfigRecordName(queueName))
	// if not found... no config existed for that queue - should not happen hashtagcrossfingers
	if err == riak.NotFound {
		// Log out an error here
		return err
	}
	val := obj.AddRegister(paramName)
	val.NewValue = []byte(value)
	// Write to Riak
	return obj.Store()
}

// HELPERS

func registerValueToString(reg *riak.RDtRegister) (string, error) {
	// The register might have been deleted at this point, so handle nil case.
	if reg == nil {
		return "", errors.New("Register is nil.")
	}
	return string(reg.Value[:]), nil
}

func (cfg *Config) RiakConnection() *riak.Client {
	return cfg.RiakPool
}

func queueConfigRecordName(queueName string) string {
	return fmt.Sprintf("queue_%s_config", queueName)
}
