package main

import (
	"flag"
	"github.com/Sirupsen/logrus"
	"github.com/Tapjoy/dynamiq/app"
)

func main() {
	//Get some Command line options
	config_file := flag.String("c", "./lib/config.gcfg", "location of config file")
	flag.Parse()

	//setup the config file
	cfg, err := app.GetCoreConfig(config_file)

	topics := app.InitTopics(cfg, cfg.Queues)
	cfg.Topics = &topics

	if err != nil {
		logrus.Fatal(err)
	}
	logrus.SetLevel(cfg.Core.LogLevel)

	list := app.InitMember(cfg)

	http_api := app.HTTP_API_V1{}

	http_api.InitWebserver(list, cfg)
}
