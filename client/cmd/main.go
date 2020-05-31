package main

import (
	"log"
	"net/rpc"

	"viewStampedReplication/server/app"
)

func main() {
	var reply *app.ClientReply
	client, err := rpc.DialHTTP("tcp", "localhost:4582")


	err = client.Call("KeyValueStore.Insert", k1v1, &reply)
	if err != nil {
		log.Fatalf("Received unexpected error running Insert; req: %v, err: %v", k1v1, err)
	}
	log.Printf("Got response running Insert; req: %v, res: %v", k1v1, reply)
	log.Printf("Got response running Get; req: %v, res: %v", "key1", reply)
}
