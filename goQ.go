package main

import (
	"flag"
	"github.com/Tapjoy/RQS/app"
	"log"
)

func main() {
	//Get some Command line options
	config_file := flag.String("c", "./lib/config.gcfg", "location of config file")
	flag.Parse()

	//setup the config file
	cfg, err := app.GetCoreConfig(config_file)
	if err != nil {
		log.Fatal(err)
	}

	list := app.InitMember(cfg)

	app.InitWebserver(list, cfg)
}
