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

const (
	indexHTML string = "server/ui/index.html"
)

var appImpl *Impl
var rpcServer *rpc.Server

type Service interface {
	Request(req *common.ClientRequest, res *common.ClientReply) error
}

type Impl struct {
	*viewmanager.Impl
	WorkQueue chan *common.ClientRequest
}

func (impl *Impl) Request(req *common.ClientRequest, res *clientrpc.EmptyResponse) error {
	req.LogRequest(true)
	reply := &common.ClientReply{
		Res: nil,
		Err: nil,
		ReplicaId: impl.Config.Id,
		DestId: req.ClientId,
	}
	c := impl.Config.GetClient(req.ClientId)
	// Ignore requests if we're in view-change or recovery mode.
	if !impl.IsClusterStatusNormal() {
		var errStr = fmt.Sprintf("invalid operation status of cluster: %s", impl.GetClusterStatus())
		reply.Err = &errStr
		reply.LogRequest(false)
		c.Do("Client.Reply", reply, &clientrpc.EmptyResponse{}, true)
		return nil
	}
	// Ignore requests if this is a backup.
	if !impl.IsPrimary() {
		var errStr = fmt.Sprintf("invalid operation status of host: %s", viewreplication.RoleBackup)
		reply.Err = &errStr
		reply.LogRequest(false)
		c.Do("Client.Reply", reply, &clientrpc.EmptyResponse{}, true)
		return nil
	}

	clientState := impl.GetClientState(req.ClientId)

	// Create client state if this is the first request from it.
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
	} else if req.RequestId <= clientState.RequestId {
		var errStr = fmt.Sprintf("stale request id detected")
		reply.Err = &errStr
		reply.LogRequest(false)
		c.Do("Client.Reply", reply, &clientrpc.EmptyResponse{}, true)
		return nil
	}
	// Add request to queue to be processed in viewreplication package
	impl.WorkQueue <- req
	return nil
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
				impl.CommitId = req.OpId
				c := impl.Config.GetClient(impl.Ops[i].Log.ClientId)
				resp := impl.Impl.UpdateClientState(impl.Ops[i].Log.ClientId, impl.Ops[i].Log.RequestId, result)
				reply := &common.ClientReply{
					Res: resp,
					Err: nil,
					ReplicaId: impl.Config.Id,
					DestId: c.Id,
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

func Init() {
	rpcServer = rpc.NewServer()
	viewmanager.Init(rpcServer)
	appImpl = &Impl{
		Impl: viewmanager.GetImpl(),
		WorkQueue:       make(chan *common.ClientRequest, clientrpc.MaxRequests),
	}
	rpcServer.RegisterName("Application", appImpl)
	go appImpl.ProcessClientRequests()
	rpcServer.HandleHTTP("/rpc", "/debug")
	listener, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", appImpl.Config.Self.GetPort()))
	if err != nil {
		log.Fatal("Listen error: ", err)
	}
	log.Printf("Listening on port %d", appImpl.Config.Self.GetPort())
	log.Print("App initialization successful")
	handler := http.NewServeMux()
	handler.HandleFunc("/", indexHandler)
	err = http.Serve(listener, handler)
	if err != nil {
		log.Fatal("Error serving: ", err)
	}
}
