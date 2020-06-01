package app

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"viewStampedReplication/clientrpc"
	"viewStampedReplication/common"
	log2 "viewStampedReplication/log"
	"viewStampedReplication/server/viewmanager"
	"viewStampedReplication/server/viewreplication"
)

var appImpl *Impl

type Service interface {
	Request(req *common.ClientRequest, res *common.ClientReply) error
}

type Impl struct {
	*viewmanager.Impl
	WorkQueue chan *common.ClientRequest
}

func GetImpl() *Impl {
	return appImpl
}

func (impl *Impl) Request(req *common.ClientRequest, res *clientrpc.EmptyResponse) error {
	req.LogRequest(true)
	reply := &common.ClientReply{
		Res: nil,
		Err: nil,
		ReplicaId: impl.Config.Id,
	}
	c := impl.Config.GetClient(req.ClientId)
	if !impl.IsClusterStatusNormal() {
		var errStr = fmt.Sprintf("invalid operation status of cluster: %s", impl.GetClusterStatus())
		reply.Err = &errStr
		reply.LogRequest(false)
		c.Do("Client.Reply", reply, &clientrpc.EmptyResponse{}, true)
		return nil
	}
	if !impl.IsPrimary() {
		var errStr = fmt.Sprintf("invalid operation status of host: %s", viewreplication.RoleBackup)
		reply.Err = &errStr
		reply.LogRequest(false)
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
			var errStr = fmt.Sprintf("stale request id detected")
			reply.Err = &errStr
			reply.LogRequest(false)
			c.Do("Client.Reply", reply, &clientrpc.EmptyResponse{}, true)
			return nil
		} else if req.RequestId == lastResponse.RequestId {
			reply.Res = clientState.LastResponse
			log.Printf("Request already executed; req: %v, res: %v", req, res)
			reply.LogRequest(false)
			c.Do("Client.Reply", reply, &clientrpc.EmptyResponse{}, true)
			return nil
		}
	}
	// Add request to queue to be processed in viewreplication package
	impl.BufferRequest(req)
	return nil
}

func (impl *Impl) BufferRequest(req *common.ClientRequest) {
	impl.WorkQueue <- req
}

func (impl *Impl) ProcessClientRequests() {
	for {
		req := <-(impl.WorkQueue)
		opId := impl.AdvanceOpId()
		op := impl.AppendOp(req, opId)
		impl.UpdateClientState(req)
		impl.SendPrepareMessages(req.ClientId, req.RequestId, op)
	}
}

func (impl *Impl) PrepareOk(req *viewreplication.PrepareOkRequest, res *clientrpc.EmptyResponse) error {
	req.LogRequest(true)
	if !impl.IsPrimary() {
		log.Printf("Not primary. Ignoring request; req: %v", req)
		return nil
	}
	for i, _ := range impl.Ops {
		// Don't process committed Ops.
		if impl.Ops[i].Committed {
			continue
		}
		if impl.Ops[i].OpId == req.OpId {
			impl.Ops[i].Quorum[req.ReplicaId] = true
			if impl.Ops[i].IsQuorumSatisfied() {
				result := impl.Ops[i].Commit()
				impl.AdvanceCommitId()
				c := impl.Config.GetClient(impl.Ops[i].Log.ClientId)
				resp := impl.Impl.UpdateClientState(impl.Ops[i].Log.ClientId, impl.Ops[i].Log.RequestId, result)
				reply := &common.ClientReply{
					Res: resp,
					Err: nil,
					ReplicaId: impl.Config.Id,
				}
				reply.LogRequest(false)
				c.Do("Client.Reply", reply, &clientrpc.EmptyResponse{}, true)
			}
		}
	}
	return nil
}

func (impl *Impl) AppendOp(req *common.ClientRequest, opId int) log2.Operation {
	logMsg := log2.LogMessage{
		ClientId:  req.ClientId,
		RequestId: req.RequestId,
		Log:       req.Op,
	}
	return impl.Impl.AppendOp(opId, logMsg)
}

func (impl *Impl) UpdateClientState(req *common.ClientRequest) *viewreplication.OpResponse {
	return impl.Impl.UpdateClientState(req.ClientId, req.RequestId, nil)
}

func GetApp() *Impl {
	return appImpl
}

func Init() {
	viewmanager.Init()
	appImpl = &Impl{
		Impl: viewmanager.GetImpl(),
		WorkQueue:       make(chan *common.ClientRequest, clientrpc.MaxRequests),
	}
	rpc.RegisterName("Application", appImpl)
	go appImpl.ProcessClientRequests()
	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", appImpl.Config.Self.GetPort()))
	if err != nil {
		log.Fatal("Listen error: ", err)
	}
	log.Printf("Listening on port %d", appImpl.Config.Self.GetPort())
	err = http.Serve(listener, nil)
	if err != nil {
		log.Fatal("Error serving: ", err)
	}
	log.Print("App initialization successful")
}
