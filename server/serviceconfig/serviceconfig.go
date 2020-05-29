package serviceconfig

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
)

var config Config

type Config struct {
    Id int `json:"-"`
	QuorumSize int `json:"quorum_size"`
	Replicas []Replica `json:"replicas"`
    Clients []Client `json:"clients"`
    Recover bool `json:"-"`
}

type Replica struct {
	Id int `json:"id"`
	Primary bool `json:"primary"`
	Port int `json:"port"`
}

type Client struct {
	Id string `json:"id"`
	Port int `json:"port"`
}

func init() {
	log.Printf("Args: %v", os.Args)
	id, err := strconv.Atoi(os.Args[1])
	if err != nil {
		panic(fmt.Sprintf("Invalid ID argument; %v", os.Args[1]))
	}
	configFile := "config.json"
	content, err := ioutil.ReadFile(configFile)
	if err != nil {
		panic(fmt.Sprintf("Failed to open %s: %v", configFile, err))
	}
	err = json.Unmarshal(content, &config)
	if err != nil {
		panic(fmt.Sprintf("Invalid config file %s: %v", configFile, err))
	}
	config.Id = id
	var recover bool
	if len(os.Args) == 3 {
		recover, err = strconv.ParseBool(os.Args[2])
		if err != nil {
			panic(fmt.Sprintf("Invalid recover argument; %v", os.Args[2]))
		}
	}
	config.Recover = recover
	log.Printf("[INFO] ServiceConfig: %+v", config)
}

func GetConfig() *Config {
	return &config
}