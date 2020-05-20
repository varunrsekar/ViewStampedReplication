package app

import (
	"log"
	"viewStampedReplication/server/clientrpc"
	"viewStampedReplication/server/viewmanager"
)

type Service interface {
	Request(req *ClientRequest, res *ClientResponse)
}

type ClientRequest struct {
	Op        string
	ClientId  string
	RequestId int
}

func (req *ClientRequest) LogRequest() {
	log.Printf("[ClientRequest] Received new client request; req: %v", req)
}

type ClientResponse struct {
	View int
	RequestId int
	Result int
	Err error
}

func (res *ClientResponse) LogResponse() {
	log.Printf("[ClientResponse] client response: %v", res)
}

type ClientState struct {
	Id string
	LastResponse *ClientResponse
}

type Impl struct {
	viewManagerImpl *viewmanager.Impl
	ClientStates map[string]*ClientState
	WorkQueue chan *clientrpc.WorkRequest
}