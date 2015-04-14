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

	if *config_file == "" {
		logrus.Warn("Empty value provided for config file location from flag -c : Falling back to default location './lib/config.gcfg'")
		*config_file = "./lib/config.gcfg"
	}

	//setup the config file
	cfg, err := app.GetCoreConfig(config_file)

	topics := app.InitTopics()
	cfg.Topics = &topics

	if err != nil {
		logrus.Fatal(err)
	}
	logrus.SetLevel(cfg.Core.LogLevel)

	http_api := app.HTTP_API_V1{}

	http_api.InitWebserver()
}
