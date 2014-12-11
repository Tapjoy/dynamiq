package stats

import (
	"github.com/quipo/statsd"
	"time"
)

type StatsClient interface {
	Incr(id string, value int64) error
	Decr(id string, value int64) error
	IncrTimer(id string, value int64) error
	DecrTimer(id string, value int64) error
}

// This client will report stats to a StatsD compatible service
type StatsdClient struct {
	prefix           string
	address          string
	unbufferedClient *statsd.StatsdClient
	interval         time.Duration
	client           *statsd.StatsdBuffer
}

func NewStatsdClient(address string, prefix string, interval time.Duration) StatsdClient {
	client := StatsdClient{
		prefix:   prefix,
		interval: interval,
		address:  address,
	}
	client.unbufferedClient = statsd.NewStatsdClient(address, prefix)
	client.client = statsd.NewStatsdBuffer(interval, client.unbufferedClient)
	return client
}

func (c StatsdClient) Incr(id string, value int64) error {
	return c.client.Incr(id, value)
}

func (c StatsdClient) Decr(id string, value int64) error {
	return c.client.Decr(id, value)
}

func (c StatsdClient) IncrTimer(id string, value int64) error {
	return c.client.Timing(id, value)
}

func (c StatsdClient) DecrTimer(id string, value int64) error {
	return c.client.Timing(id, -value)
}

// This client is to sub in when we don't want to write stats
type NOOPClient struct {
}

func NewNOOPClient() NOOPClient {
	return NOOPClient{}
}

func (c NOOPClient) Incr(id string, value int64) error {
	return nil
}

func (c NOOPClient) Decr(id string, value int64) error {
	return nil
}

func (c NOOPClient) IncrTimer(id string, value int64) error {
	return nil
}

func (c NOOPClient) DecrTimer(id string, value int64) error {
	return nil
}
