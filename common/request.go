package common

import (
	"fmt"
	"github.com/davecgh/go-spew/spew"
	"viewStampedReplication/clientrpc"
	log2 "viewStampedReplication/log"
	"viewStampedReplication/server/viewreplication"
)


type ClientRequest struct {
	Op        log2.Message
	ClientId  string
	RequestId int
	DestId int
}

func (req *ClientRequest) LogRequest(recv bool) {
	clientrpc.Log(fmt.Sprintf("[ClientRequest] %s new client request; req: %+v", "%s", spew.NewFormatter(req)), recv)
}

type ClientReply struct {
	Res *viewreplication.OpResponse
	Err *string
	ReplicaId int
	DestId string
}

func (req *ClientReply) LogRequest(recv bool) {
	clientrpc.Log(fmt.Sprintf("[ClientReply] %s client reply: %+v", "%s", spew.NewFormatter(req)), recv)
}