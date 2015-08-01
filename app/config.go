package app

import (
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"code.google.com/p/gcfg"
	"github.com/Sirupsen/logrus"
	"github.com/Tapjoy/dynamiq/app/compressor"
	"github.com/Tapjoy/dynamiq/app/stats"
	"github.com/tpjg/goriakpbc"
)

var (
	// ErrConfigurationOptionNotFound represents the condition that occurs if an invalid
	// location is specified for the config file
	ErrConfigurationOptionNotFound = errors.New("Configuration Value Not Found")
)

// ConfigurationBucket is the name of the riak bucket holding the config
const ConfigurationBucket = "config"

// QueueConfigName is the key in the riak bucket holding the config
const QueueConfigName = "queue_config"

// QueueSetName is the crdt key holding the set of all queues
const QueueSetName = "queues"

// VisibilityTimeout is the name of the config setting name for controlling how long a message is "inflight"
const VisibilityTimeout = "visibility_timeout"

// PartitionCount is
const PartitionCount = "partition_count"

// MinPartitions is the name of the config setting name for controlling the minimum number of partitions per queue
const MinPartitions = "min_partitions"

// MaxPartitions is the name of the config setting name for controlling the maximum number of partitions per node
const MaxPartitions = "max_partitions"

// MaxPartitionAge is the name of the config setting name for controlling how long an un-used partition should exist
const MaxPartitionAge = "max_partition_age"

// CompressedMessages is the name of the config setting name for controlling if the queue is using compression or not
const CompressedMessages = "compressed_messages"

// Settings Arrays and maps cannot be made immutable in golang
var Settings = [...]string{VisibilityTimeout, PartitionCount, MinPartitions, MaxPartitions, MaxPartitionAge, CompressedMessages}

// DefaultSettings is
var DefaultSettings = map[string]string{VisibilityTimeout: "30", PartitionCount: "5", MinPartitions: "1", MaxPartitions: "10", MaxPartitionAge: "432000", CompressedMessages: "false"}

// Config is
type Config struct {
	Core       Core
	Stats      Stats
	Compressor compressor.Compressor
	Queues     *Queues
	RiakPool   *riak.Client
	Topics     *Topics
}

// Core is
type Core struct {
	Name                  string
	Port                  int
	SeedServer            string
	SeedPort              int
	SeedServers           []string
	HTTPPort              int
	RiakNodes             string
	BackendConnectionPool int
	SyncConfigInterval    time.Duration
	LogLevel              logrus.Level
	LogLevelString        string
}

// Stats is
type Stats struct {
	Type          string
	FlushInterval int
	Address       string
	Prefix        string
	Client        stats.Client
}

func initRiakPool(cfg *Config) *riak.Client {
	rand.Seed(time.Now().UnixNano())
	// TODO this should just be 1 HAProxy
	hosts := []string{cfg.Core.RiakNodes}
	host := hosts[rand.Intn(len(hosts))]
	return riak.NewClientPool(host, cfg.Core.BackendConnectionPool)
}

// GetCoreConfig is
func GetCoreConfig(configFile *string) (*Config, error) {
	var cfg Config
	err := gcfg.ReadFileInto(&cfg, *configFile)
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

	go cfg.Queues.scheduleSync(&cfg)
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
	configBucket, err := client.NewBucketType("maps", ConfigurationBucket)
	if err != nil {
		// most commonly, the error here relates to a fundamental issue talking to riak
		// likely, the connection pool is larger than the allowable number of file handles
		logrus.Errorf("Error trying to get maps bucket type: %s", err)
	}
	// Fetch the object for holding the set of queues
	config, err := configBucket.FetchMap(QueueConfigName)
	if err != nil {
		logrus.Errorf("Error trying to get queue config bucket: %s", err)
	}
	queuesConfig.Config = config

	// AddSet implicitly calls fetch set if the set already exists
	queueSet := config.AddSet(QueueSetName)
	if queueSet == nil {
		queueSet.Add([]byte("default_queue"))
		config.Store()
		config, _ = configBucket.FetchMap(QueueConfigName)
	}
	// For each queue we have in the system
	for _, elem := range queueSet.GetValue() {
		// Convert it's name into a string
		name := string(elem[:])
		// Get the Riak RdtMap of Settings for this queue
		configMap, _ := configBucket.FetchMap(queueConfigRecordName(name))
		// Pre-warm the Settings object
		queue := &Queue{
			Name:   name,
			Config: configMap,
			Parts:  InitPartitions(cfg, name),
		}
		// TODO: We should be handling errors here
		// Set the queue in the queue map
		queuesConfig.QueueMap[name] = queue
	}
	// Return the completed Queue cache of Settings
	return &queuesConfig
}

// InitializeQueue is
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
	bucket, _ := client.NewBucketType("maps", ConfigurationBucket)
	queueConfig, _ := bucket.FetchMap(QueueConfigName)
	queueSet := queueConfig.AddSet(QueueSetName)
	queueSet.Add([]byte(queueName))
	return queueConfig.Store()
}

func (cfg *Config) removeFromKnownQueues(queueName string) error {
	// If we disallow topicless-queues, we can remove this and put it into Topic.RemoveQueue
	client := cfg.RiakConnection()
	// We purposefully read from Riak here, we'll enventually-consist with the in memory cache
	bucket, _ := client.NewBucketType("maps", ConfigurationBucket)
	queueConfig, _ := bucket.FetchMap(QueueConfigName)
	queueSet := queueConfig.AddSet(QueueSetName)
	queueSet.Remove([]byte(queueName))
	return queueConfig.Store()
}

// TODO: Take in a map which overrides the defaults
func (cfg *Config) createConfigForQueue(queueName string) (*riak.RDtMap, error) {
	client := cfg.RiakConnection()
	// Get the bucket for holding maps of config data
	// TODO: Find a nice way to DRY this up - it's a lil copy/pasty
	bucket, _ := client.NewBucketType("maps", ConfigurationBucket)
	// Get the object for this queues Settings
	obj, _ := bucket.FetchMap(queueConfigRecordName(queueName))
	// For each known setting
	for _, elem := range Settings {
		// Get the reigster for this setting
		reg := obj.AddRegister(elem)
		// Convert the default value to a bytearray, set it on the Register
		reg.Update([]byte(DefaultSettings[elem]))
	}
	// Save the object, returns an error up the callchain if needed
	return obj, obj.Store()
}

// SETTERS AND GETTERS FOR QUEUE CONFIG

// GetVisibilityTimeout is
func (cfg *Config) GetVisibilityTimeout(queueName string) (float64, error) {
	val, err := cfg.getQueueSetting(VisibilityTimeout, queueName)
	parsed, err := strconv.ParseFloat(val, 32)
	return parsed, err
}

// SetVisibilityTimeout is
func (cfg *Config) SetVisibilityTimeout(queueName string, timeout float64) error {
	return cfg.setQueueSetting(VisibilityTimeout, queueName, strconv.FormatFloat(timeout, 'f', -1, 64))
}

// GetMinPartitions is
func (cfg *Config) GetMinPartitions(queueName string) (int, error) {
	val, _ := cfg.getQueueSetting(MinPartitions, queueName)
	return strconv.Atoi(val)
}

// SetMinPartitions is
func (cfg *Config) SetMinPartitions(queueName string, timeout int) error {
	// TODO do we handle any resizing here? Or does the system "self-adjust"
	return cfg.setQueueSetting(MinPartitions, queueName, strconv.Itoa(timeout))
}

// GetMaxPartitions is
func (cfg *Config) GetMaxPartitions(queueName string) (int, error) {
	val, _ := cfg.getQueueSetting(MaxPartitions, queueName)
	return strconv.Atoi(val)
}

// SetMaxPartitions is
func (cfg *Config) SetMaxPartitions(queueName string, timeout int) error {
	// TODO do we handle any resizing here? Or does the system "self-adjust"
	return cfg.setQueueSetting(MaxPartitions, queueName, strconv.Itoa(timeout))
}

// SetMaxPartitionAge is
func (cfg *Config) SetMaxPartitionAge(queueName string, age float64) error {
	return cfg.setQueueSetting(MaxPartitionAge, queueName, strconv.FormatFloat(age, 'f', -1, 64))
}

// GetMaxPartitionAge is
func (cfg *Config) GetMaxPartitionAge(queueName string) (float64, error) {
	val, _ := cfg.getQueueSetting(MaxPartitionAge, queueName)
	return strconv.ParseFloat(val, 32)
}

// GetCompressedMessages is
func (cfg *Config) GetCompressedMessages(queueName string) (bool, error) {
	val, _ := cfg.getQueueSetting(CompressedMessages, queueName)
	return strconv.ParseBool(val)
}

// SetCompressedMessages is
func (cfg *Config) SetCompressedMessages(queueName string, compressedMessages bool) error {
	return cfg.setQueueSetting(CompressedMessages, queueName, strconv.FormatBool(compressedMessages))
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
			regValue := cfg.Queues.QueueMap[queueName].getConfig().FetchRegister(paramName)
			if regValue != nil {
				value, err = registerValueToString(regValue)
				if err != nil {
					return value, err
				}
			} else {
				// There is a chance the queue pre-dated the existence of the given parameter. If so, use the
				// configured default value for now
				// TODO - Need to backfill missing params when they're detected
				value = DefaultSettings[paramName]
			}
		}
	}

	if value == "" {
		// Read from riak
		client := cfg.RiakConnection()
		bucket, _ := client.NewBucketType("maps", ConfigurationBucket)
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
	bucket, _ := client.NewBucketType("maps", ConfigurationBucket)
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

// RiakConnection returns a pointer to the current pool of riak connections, which
// is abstracted inside of the riak.Client object
func (cfg *Config) RiakConnection() *riak.Client {
	return cfg.RiakPool
}

func queueConfigRecordName(queueName string) string {
	return fmt.Sprintf("queue_%s_config", queueName)
}
