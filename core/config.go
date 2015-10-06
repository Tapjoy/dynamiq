package core

import (
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
}

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
		ConfigSyncInterval: 30 * time.Second,
	}

	rs, err := NewRiakService(riakConfig.Addresses, riakConfig.Port)
	if err != nil {
		return nil, err
	}
	riakConfig.Service = rs
	return &Config{
		Riak:      riakConfig,
		Discovery: discoveryConfig,
		HTTP:      httpConfig,
	}, nil
}
