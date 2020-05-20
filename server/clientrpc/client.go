package clientrpc

import (
	"log"
	"net/rpc"
	"sync"
)

type Client struct {
	sync.Mutex
	c *rpc.Client
	Hostname string
}

type Request interface {
	LogRequest()
}

type Response interface {
	LogResponse()
}

type WorkRequest struct {
	req Request
	wg *sync.WaitGroup
	result Response
}

func (wr *WorkRequest) GetWG() *sync.WaitGroup {
	return wr.wg
}

func (wr *WorkRequest) GetRequest() Request {
	return wr.req
}

func (wr *WorkRequest) SetResult(res Response) {
	wr.result = res
}

func (wr *WorkRequest) GetResult() Response {
	return wr.result
}

func NewWorkRequest(req Request, wg *sync.WaitGroup) *WorkRequest {
	return &WorkRequest{
		req:    req,
		wg:     wg,
		result: nil,
	}
}
func (client *Client) prepare() {
	if client.c != nil {
		return
	}
	client.Lock()
	defer client.Unlock()
	var err error
	client.c, err = rpc.DialHTTP("tcp", client.Hostname)
	if err != nil {
		log.Fatalf("Failed to create RPC client to host %s due to error '%v'", client.Hostname, err)
	}
}

func (client *Client) Do(api string, req Request, res Response) {
	client.prepare()
	err := client.c.Call(api, req, res)
	if err != nil {
		log.Printf("Received error from RPC request; api: %s, req: %s, err: %v", api, req, err)
	}
}

func (client *Client) AsyncDo(api string, req Request, res Response) {
	client.prepare()
	err := client.c.Go(api, req, res, nil)
	if err != nil {
		log.Printf("Received error from async RPC request; api: %s, req: %s, err: %v", api, req, err)
	}
}