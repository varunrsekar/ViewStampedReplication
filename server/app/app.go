package app

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"viewStampedReplication/server/clientrpc"
	log2 "viewStampedReplication/server/log"
	"viewStampedReplication/server/viewmanager"
	"viewStampedReplication/server/viewreplication"
)

var appImpl *Impl

type Service interface {
	Request(req *ClientRequest, res *ClientReply) error
}

type Impl struct {
	*viewmanager.Impl
	WorkQueue chan *ClientRequest
}

func GetImpl() *Impl {
	return appImpl
}

func (impl *Impl) Request(req *ClientRequest, res *clientrpc.EmptyResponse) error {
	req.LogRequest()
	reply := &ClientReply{
		Res: nil,
		Err: nil,
	}
	c := impl.Config.GetClient(req.ClientId)
	if !impl.IsClusterStatusNormal() {
		reply.Err = fmt.Errorf("invalid operation status of cluster: %s", impl.GetClusterStatus())
		c.Do("Client.Reply", reply, &clientrpc.EmptyResponse{}, true)
		return nil
	}
	if !impl.IsPrimary() {
		reply.Err = fmt.Errorf("invalid operation status of host: %s", viewreplication.RoleBackup)
		c.Do("Client.Reply", reply, &clientrpc.EmptyResponse{}, true)
		return nil
	}

	clientState := impl.GetClientState(req.ClientId)
	if clientState == nil {
		clientState = impl.CreateClientState(req.ClientId)
	}
	lastResponse := clientState.LastResponse
	if lastResponse != nil {
		if req.RequestId < lastResponse.RequestId {
			reply.Err = fmt.Errorf("stale request id detected")
			c.Do("Client.Reply", reply, &clientrpc.EmptyResponse{}, true)
			return nil
		} else if req.RequestId == lastResponse.RequestId {
			reply.Res = clientState.LastResponse
			log.Printf("Request already executed; req: %v, res: %v", req, res)
			c.Do("Client.Reply", reply, &clientrpc.EmptyResponse{}, true)
			return nil
		}
	}
	// Add request to queue to be processed in viewreplication package
	impl.BufferRequest(req)
	return nil
}

func (impl *Impl) BufferRequest(req *ClientRequest) {
	impl.WorkQueue <- req
}

func (impl *Impl) ProcessClientRequests() {
	for {
		req := <-(impl.WorkQueue)
		log.Printf("Received work request; req: %v", req)
		opId := impl.AdvanceOpId()
		op := impl.AppendOp(req, opId)
		impl.UpdateClientState(req)
		impl.SendPrepareMessages(req.ClientId, req.RequestId, op)
	}
}

func (impl *Impl) PrepareOk(req *viewreplication.PrepareOkRequest, res *clientrpc.EmptyResponse) error {
	if !impl.IsPrimary() {
		log.Printf("Not primary. Ignoring request; req: %v", req)
		return nil
	}
	for _, op := range impl.Ops {
		if op.OpId == req.OpId {
			op.Quorum[req.ReplicaId] = true
			if op.IsQuorumSatisfied() {
				result := op.Commit()
				impl.AdvanceCommitId()
				c := impl.Config.GetClient(op.Log.ClientId)
				resp := impl.Impl.UpdateClientState(op.Log.ClientId, op.Log.RequestId, result)
				reply := &ClientReply{
					Res: resp,
					Err: nil,
				}
				c.Do("Client.Reply", reply, &clientrpc.EmptyResponse{}, true)
			}
		}
	}
	return nil
}

func (impl *Impl) AppendOp(req *ClientRequest, opId int) log2.Operation {
	logMsg := log2.LogMessage{
		ClientId:  req.ClientId,
		RequestId: req.RequestId,
		Log:       req.Op,
	}
	return impl.Impl.AppendOp(opId, logMsg)
}

func (impl *Impl) UpdateClientState(req *ClientRequest) *viewreplication.OpResponse {
	return impl.Impl.UpdateClientState(req.ClientId, req.RequestId, nil)
}

func GetApp() *Impl {
	return appImpl
}

func Init() {
	viewmanager.Init()
	appImpl = &Impl{
		Impl: viewmanager.GetImpl(),
		WorkQueue:       make(chan *ClientRequest, clientrpc.MaxRequests),
	}
	rpc.RegisterName("Application", appImpl)
	log.Print("App initialization successful")
	go appImpl.ProcessClientRequests()
	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", appImpl.Config.Self.GetPort()))
	if err != nil {
		log.Fatal("Listen error: ", err)
	}
	err = http.Serve(listener, nil)
	if err != nil {
		log.Fatal("Error serving: ", err)
	}
}
