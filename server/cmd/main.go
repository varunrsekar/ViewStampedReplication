package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"

	"viewStampedReplication/common"
)

type KeyValueStore struct {
	rwm sync.RWMutex
	cache map[string]common.Value
}

func NewKeyValueStore() *KeyValueStore {
	return &KeyValueStore{
		cache: make(map[string]common.Value),
	}
}
func (k *KeyValueStore) Insert(kv common.KeyValue, reply *common.Value) error {
	k.rwm.Lock()
	defer k.rwm.Unlock()
	key, val := kv.Key, kv.Value
	_, exists := k.cache[key]
	if exists {
		return fmt.Errorf("key '%s' already exists", key)
	}
	k.cache[key] = common.Value{
		Val: val,
	}
	*reply = k.cache[key]
	return nil
}

func (k *KeyValueStore) Get(key string, reply *common.Value) error {
	k.rwm.RLock()
	defer k.rwm.RUnlock()
	value, exists := k.cache[key]
	if !exists {
		return fmt.Errorf("key '%s' not found", key)
	}
	*reply = value
	return nil
}

func main() {
	kvs := NewKeyValueStore()
	err := rpc.Register(kvs)
	if err != nil {
		log.Fatal("Failed to register RPC; err: ", err)
	}
	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", ":4582")
	if err != nil {
		log.Fatal("Failed to create RPC listener; err: ", err)
	}
	log.Printf("Listening on port %d", 4582)
	err = http.Serve(listener, nil)
	if err != nil {
		log.Fatal("Failed to serve HTTP; err: ", err)
	}

}
