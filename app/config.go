package app

import (
	"code.google.com/p/gcfg"
	"errors"
	"fmt"
	"github.com/Sirupsen/logrus"
	"github.com/Tapjoy/dynamiq/app/compressor"
	"github.com/Tapjoy/dynamiq/app/stats"
	"github.com/hashicorp/memberlist"
	"github.com/tpjg/goriakpbc"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

var (
	ConfigurationOptionNotFound = errors.New("Configuration Value Not Found")
	config                      = &Config{}
)

func GetConfig() *Config {
	return config
}

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
	Core        Core
	Stats       Stats
	Compressor  compressor.Compressor
	Queues      *Queues
	RiakPool    *riak.Client
	Topics      *Topics
	MemberNodes *memberlist.Memberlist
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
	Type          string
	FlushInterval int
	Address       string
	Prefix        string
	Client        stats.StatsClient
}

func initRiakPool() {
	rand.Seed(time.Now().UnixNano())
	// TODO this should just be 1 HAProxy
	hosts := []string{config.Core.RiakNodes}
	host := hosts[rand.Intn(len(hosts))]
	config.RiakPool = riak.NewClientPool(host, config.Core.BackendConnectionPool)
	return
}

func GetCoreConfig(config_file *string) (*Config, error) {
	err := gcfg.ReadFileInto(config, *config_file)
	if err != nil {
		logrus.Fatal(err)
	}

	if len(config.Core.SeedServer) == 0 {
		logrus.Fatal("The list of seedservers was empty")
	}

	config.Core.SeedServers = strings.Split(config.Core.SeedServer, ",")
	for i, x := range config.Core.SeedServers {
		config.Core.SeedServers[i] = x + ":" + strconv.Itoa(config.Core.SeedPort)
	}

	// This will join the node to the cluster
	config.MemberNodes, _, err = InitMemberList(config.Core.Name, config.Core.Port, config.Core.SeedServers, config.Core.SeedPort)
	if err != nil {
		logrus.Error(err)
	}

	// This will place a fully prepared riak connection pool onto the config object
	initRiakPool()

	// This will load all the queue config from Riak and place it onto the config object
	loadQueuesConfig()

	switch config.Stats.Type {
	case "statsd":
		config.Stats.Client = stats.NewStatsdClient(config.Stats.Address, config.Stats.Prefix, time.Second*time.Duration(config.Stats.FlushInterval))
	default:
		config.Stats.Client = stats.NewNOOPClient()
	}

	// Currently we only support zlib, but we may support others
	// Here is where we'd detect and inject
	config.Compressor = compressor.NewZlibCompressor()

	config.Core.LogLevel, err = logrus.ParseLevel(config.Core.LogLevelString)
	if err != nil {
		logrus.Fatal(err)
	}

	go config.Queues.syncConfig()
	return config, err
}

func loadQueuesConfig() {
	// Create the Queues Config struct
	queuesConfig := Queues{
		QueueMap: make(map[string]*Queue),
	}
	// Get the queues
	client := config.RiakConnection()
	// TODO: We should be handling errors here
	// Get the bucket holding the map of config data
	configBucket, err := client.NewBucketType("maps", CONFIGURATION_BUCKET)
	if err != nil {
		// most commonly, the error here relates to a fundamental issue talking to riak
		// likely, the connection pool is larger than the allowable number of file handles
		logrus.Errorf("Error trying to get maps bucket type: %s", err)
	}
	// Fetch the object for holding the set of queues
	qconfigObj, err := configBucket.FetchMap(QUEUE_CONFIG_NAME)
	if err != nil {
		logrus.Errorf("Error trying to get queue config bucket: %s", err)
	}
	queuesConfig.Config = qconfigObj

	// AddSet implicitly calls fetch set if the set already exists
	queueSet := qconfigObj.AddSet(QUEUE_SET_NAME)
	if queueSet == nil {
		queueSet.Add([]byte("default_queue"))
		qconfigObj.Store()
		qconfigObj, _ = configBucket.FetchMap(QUEUE_CONFIG_NAME)
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
			Parts:  InitPartitions(name),
		}
		// TODO: We should be handling errors here
		// Set the queue in the queue map
		queuesConfig.QueueMap[name] = queue
	}
	// Set the completed Queue cache of settings
	config.Queues = &queuesConfig
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
		Parts:  InitPartitions(queueName),
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
