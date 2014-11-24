package app

import (
	"code.google.com/p/gcfg"
	"log"
	"time"
)

// The Config / ConfigCore relationship is a required aspect of gcfg
// It defines configuration in terms of sections, Core being the primary config
// needed to start the process itself
type Config struct {
	Core Core
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

func GetCoreConfig(config_file *string) (Config, error) {
	var cfg Config
	err := gcfg.ReadFileInto(&cfg, *config_file)
	if err != nil {
		log.Fatal(err)
	}
	return cfg, err
}
