package app

import (
	"log"
	log2 "viewStampedReplication/server/log"
	"viewStampedReplication/server/viewreplication"
)

type ClientRequest struct {
	Op        log2.Message
	ClientId  string
	RequestId int
}

func (req *ClientRequest) LogRequest() {
	log.Printf("[ClientRequest] Received new client request; req: %+v", req)
}

type ClientReply struct {
	Res *viewreplication.OpResponse
	Err error
}

func (res *ClientReply) LogResponse() {
	log.Printf("[ClientReply] client reply: %+v", res)
}

func (req *ClientReply) LogRequest() {
	log.Printf("[ClientReply] client reply: %+v", req)
}