package serviceconfig

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

var config Config

type Config struct {
    Id int `json:"id"`
	QuorumSize int `json:"quorum_size"`
	Replicas []Replica `json:"replicas"`
}

type Replica struct {
	Id int `json:"id"`
	Primary bool `json:"primary"`
	Port int `json:"port"`
}

func init() {
	configFile := "config.yaml"
	content, err := ioutil.ReadFile(configFile)
	if err != nil {
		panic(fmt.Sprintf("Failed to open %s: %v", configFile, err))
	}
	err = json.Unmarshal(content, config)
	if err != nil {
		panic(fmt.Sprintf("Invalid config file %s: %v", configFile, err))
	}
}

func GetConfig() *Config {
	return &config
}