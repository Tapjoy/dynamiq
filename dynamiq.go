package main

import (
	"flag"
	"log"

	"github.com/Sirupsen/logrus"
	"github.com/Tapjoy/dynamiq/app"
	"github.com/Tapjoy/dynamiq/core"
	"github.com/Tapjoy/dynamiq/server/http/v2"
)

func main2() {
	cfg, err := core.GetConfig()
	if err != nil {
		log.Fatal(err)
	}

	httpServer, err := httpv2.New(cfg)
	if err != nil {
		log.Println(err)
	}
	httpServer.Listen()
}

func main() {
	main2()
	return
	//Get some Command line options
	configFile := flag.String("c", "./lib/config.gcfg", "location of config file")
	flag.Parse()

	if *configFile == "" {
		logrus.Warn("Empty value provided for config file location from flag -c : Falling back to default location './lib/config.gcfg'")
		*configFile = "./lib/config.gcfg"
	}

	//setup the config file
	cfg, err := app.GetCoreConfig(configFile)

	cfg.Topics = app.InitTopics(cfg, cfg.Queues)

	if err != nil {
		logrus.Fatal(err)
	}
	logrus.SetLevel(cfg.Core.LogLevel)

	list, _, err := app.InitMemberList(cfg.Core.Name, cfg.Core.Port, cfg.Core.SeedServers, cfg.Core.SeedPort)
	httpAPI := app.HTTPApiV1{}

	httpAPI.InitWebserver(list, cfg)
}
