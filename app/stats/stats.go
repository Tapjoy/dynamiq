package stats

import (
	"time"

	"github.com/quipo/statsd"
)

// Client represents the interface for a set of operations that can be performed
// to track performance of queues and topics in Dynamiq
type Client interface {
	Incr(id string, value int64) error
	Decr(id string, value int64) error
	IncrGauge(id string, value int64) error
	DecrGauge(id string, value int64) error
	SetGauge(id string, value int64) error
}

// StatsdClient will report stats to a StatsD compatible service
type StatsdClient struct {
	prefix   string
	address  string
	client   *statsd.StatsdClient
	interval time.Duration
}

// NewStatsdClient will create a new StatsdClient to be used for reporting metrics
func NewStatsdClient(address string, prefix string, interval time.Duration) StatsdClient {
	client := StatsdClient{
		prefix:   prefix,
		interval: interval,
		address:  address,
	}
	client.client = statsd.NewStatsdClient(address, prefix)
	client.client.CreateSocket()
	return client
}

// Incr increases the value of a given counter
func (c StatsdClient) Incr(id string, value int64) error {
	return c.client.Incr(id, value)
}

// Decr decreases the value of a given counter
func (c StatsdClient) Decr(id string, value int64) error {
	return c.client.Decr(id, value)
}

// IncrGauge increases the value of a given gauge delta
func (c StatsdClient) IncrGauge(id string, value int64) error {
	return c.client.GaugeDelta(id, value)
}

// DecrGauge decreases the value of a given gauge delta
func (c StatsdClient) DecrGauge(id string, value int64) error {
	return c.client.GaugeDelta(id, -value)
}

// SetGauge sets the level of the given gauge
func (c StatsdClient) SetGauge(id string, value int64) error {
	return c.client.Gauge(id, value)
}

// NOOPClient is to sub in when we don't want to write stats
type NOOPClient struct {
}

// NewNOOPClient returns a new NOOPClient
func NewNOOPClient() NOOPClient {
	return NOOPClient{}
}

// Incr does nothing
func (c NOOPClient) Incr(id string, value int64) error {
	return nil
}

// Decr does nothing
func (c NOOPClient) Decr(id string, value int64) error {
	return nil
}

// IncrGauge does nothing
func (c NOOPClient) IncrGauge(id string, value int64) error {
	return nil
}

// DecrGauge does nothing
func (c NOOPClient) DecrGauge(id string, value int64) error {
	return nil
}

// SetGauge does nothing
func (c NOOPClient) SetGauge(id string, value int64) error {
	return nil
}
