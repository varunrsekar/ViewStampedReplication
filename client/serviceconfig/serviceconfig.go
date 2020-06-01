package serviceconfig

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"viewStampedReplication/logger"
)

var config Config

type Config struct {
    Id string `json:"-"`
    Port int `json:"-"`
	QuorumSize int `json:"quorum_size"`
	Replicas []Replica `json:"replicas"`
}

type Replica struct {
	Id int `json:"id"`
	Port int `json:"port"`
}

func Init() {
	log.Printf("Args: %v", os.Args)
	configFile := "config.json"
	content, err := ioutil.ReadFile(configFile)
	if err != nil {
		panic(fmt.Sprintf("Failed to open %s: %v", configFile, err))
	}
	err = json.Unmarshal(content, &config)
	if err != nil {
		panic(fmt.Sprintf("Invalid config file %s: %v", configFile, err))
	}
	config.Id = os.Args[1]
	// Initialize requests logger.
	logger.Init(fmt.Sprintf("request-%s.log", config.Id))
	config.Port, _ = strconv.Atoi(os.Args[2])
	log.Printf("[INFO] ServiceConfig: %+v", config)
}

func GetConfig() *Config {
	return &config
}