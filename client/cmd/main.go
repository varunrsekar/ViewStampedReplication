package main

import (
	"fmt"
	"log"
	"net/rpc"

	"viewStampedReplication/common"
)

func main() {
	var reply common.Value
	client, err := rpc.DialHTTP("tcp", "localhost:4582")
	if err != nil {
		log.Fatalf("Failed to connect to HTTP viewStampedReplication at port %d; err: %v", 4582, err)
	}
	k1v1 := common.KeyValue{
		Key: "key1",
		Value: "val1",
	}
	k1v2 := common.KeyValue{
		Key: "key1",
		Value: "val2",
	}

	err = client.Call("KeyValueStore.Insert", k1v1, &reply)
	if err != nil {
		log.Fatalf("Received unexpected error running Insert; req: %v, err: %v", k1v1, err)
	}
	log.Printf("Got response running Insert; req: %v, res: %v", k1v1, reply)
	err = client.Call("KeyValueStore.Insert", k1v2, &reply)
	if err == nil {
		log.Fatalf("Did not receive expected error running Insert; req: %v, exp: %v, err: %v", k1v2, fmt.Errorf("key '%s' already exists", "key1"), err)
	}
	log.Printf("Got response running Insert; req: %v, res: %v", k1v2, reply)
	err = client.Call("KeyValueStore.Get", "key2", &reply)
	if err == nil {
		log.Fatalf("Did not receive expected error running Get; exp: %v, err: %v", fmt.Errorf("key '%s' not found", "key2"), err)
	}
	log.Printf("Got response running Get; req: %v, res: %v", "key2", reply)
	err = client.Call("KeyValueStore.Get", "key1", &reply)
	if err != nil {
		log.Fatalf("Received unexpected error running Get; err: %v", err)
	}
	log.Printf("Got response running Get; req: %v, res: %v", "key1", reply)
}
