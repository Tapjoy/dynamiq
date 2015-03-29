package main

import (
	"flag"
	"github.com/Sirupsen/logrus"
	"github.com/Tapjoy/dynamiq/app"
	"github.com/Tapjoy/dynamiq/app/partitioner"
	"time"
)

func main() {
	//Get some Command line options
	config_file := flag.String("c", "./lib/config.gcfg", "location of config file")
	flag.Parse()

	//setup the config file
	cfg, err := app.GetCoreConfig(config_file)
	if err != nil {
		logrus.Fatal(err)
	}
	logrus.SetLevel(cfg.Core.LogLevel)

	list := app.InitMember(cfg)

	http_api := app.HTTP_API_V1{}

	timedRingCache := partitioner.NewTimedRingCache(time.Second*1, time.Millisecond*1000)

	go func() {
		for i := 0; i < 35; i++ {
			x, y := timedRingCache.ReserveRange(1, 200, 7)
			logrus.Info("AAAAAAAAA Range is ", x, y)
			time.Sleep(time.Millisecond * 100)
		}
	}()

	go func() {
		for i := 0; i < 35; i++ {
			x, y := timedRingCache.ReserveRange(1, 200, 7)
			logrus.Info("BBBBBBBBBB Range is ", x, y)
			time.Sleep(time.Millisecond * 100)
		}
	}()

	go func() {
		for i := 0; i < 35; i++ {
			x, y := timedRingCache.ReserveRange(1, 200, 7)
			logrus.Info("CCCCCCCCCCCC Range is ", x, y)
			time.Sleep(time.Millisecond * 100)
		}
	}()

	http_api.InitWebserver(list, cfg)
}
