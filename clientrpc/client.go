package clientrpc

import (
	"fmt"
	"log"
	"net/rpc"
	"sync"
	"viewStampedReplication/logger"
)

const MaxRequests = 100

type Client struct {
	sync.Mutex
	c *rpc.Client
	Hostname string
}

type Request interface {
	LogRequest(recv bool)
}

type Response interface {
	LogResponse(recv bool)
}

type EmptyResponse struct {}

func Log(log string, recv bool) {
	var action string
	if recv {
		action = "Received"
	} else {
		action = "Sending"
	}
	logger.GetLogger().Infof(fmt.Sprintf(log, action))
}

func (res *EmptyResponse) LogResponse(recv bool) {
	Log("[EmptyResponse] %s empty response", recv)
}

type WorkRequest struct {
	req    Request
	wg     *sync.WaitGroup
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
	/*if client.c != nil {
		return nil
	}*/
	log.Printf("Preparing rpc client; client: %v", client)
	var err error
	client.c, err = rpc.DialHTTP("tcp", client.Hostname)
	if err != nil {
		log.Printf("[error] Failed to create RPC client to host %s due to error '%v'. Skipping request", client.Hostname, err)
		return err
	}
	log.Printf("Successfully connected to host %s", client.Hostname)
	return nil
}

func (client *Client) Do(api string, req Request, res Response) error {
	if err := client.prepare(); err != nil {
		return err
	}
	err := client.c.Call(api, req, res)
	if err != nil {
		log.Printf("[error] Received error from RPC request; api: %s, req: %s, err: %v", api, req, err)
		err = client.c.Call(api, req, res)
		return err
	}
	return nil
}

func (client *Client) AsyncDo(api string, req Request, res Response) error {
	if err := client.prepare(); err != nil {
		return err
	}
	call := client.c.Go(api, req, res, nil)
	if call.Error != nil {
		log.Printf("Received error from async RPC request; api: %s, req: %v, err: %v", api, req, call.Error)
		call = client.c.Go(api, req, res, nil)
		return call.Error
	}
	return nil
}