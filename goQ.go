package main

import "log"
import "flag"
import "./app/."

func main() {
	//Get some Command line options
	config_file := flag.String("c", "./lib/config.gcfg", "location of config file")
	flag.Parse()

	//setup the config file
	cfg, err := app.Getconfig(config_file)
	if err != nil {
		log.Fatal(err)
	}

	list := app.InitMember(cfg)

	app.InitWebserver(list, cfg)
}
