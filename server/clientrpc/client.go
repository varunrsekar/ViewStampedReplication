package clientrpc

import (
	"log"
	"net/rpc"
	"sync"
)

const MaxRequests = 100

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

type EmptyResponse struct {}

func (res *EmptyResponse) LogResponse() {
	log.Print("[EmptyResponse] Empty response")
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
func (client *Client) prepare() error {
	if client.c != nil {
		return nil
	}
	log.Printf("Preparing rpc client; client: %v", client)
	client.Lock()
	defer client.Unlock()
	var err error
	client.c, err = rpc.DialHTTP("tcp", client.Hostname)
	if err != nil {
		log.Printf("[error] Failed to create RPC client to host %s due to error '%v'. Skipping request", client.Hostname, err)
		return err
	}
	return nil
}

func (client *Client) Do(api string, req Request, res Response) {
	if client.prepare() != nil {
		return
	}
	err := client.c.Call(api, req, res)
	if err != nil {
		log.Printf("Received error from RPC request; api: %s, req: %s, err: %v", api, req, err)
	}
}

func (client *Client) AsyncDo(api string, req Request, res Response) {
	if client.prepare() != nil {
		return
	}
	call := client.c.Go(api, req, res, nil)
	if call.Error != nil {
		log.Printf("Received error from async RPC request; api: %s, req: %v, err: %v", api, req, call.Error)
	}
}