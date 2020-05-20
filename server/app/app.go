package app

import (
	"fmt"
	"log"
	"net/rpc"
	"sync"
	"viewStampedReplication/server/clientrpc"
	log2 "viewStampedReplication/server/log"
	"viewStampedReplication/server/viewmanager"
	"viewStampedReplication/server/viewreplication"
)

var appImpl *Impl

const (
	maxRequests = 100
)
func GetImpl() *Impl {
	return appImpl
}

func (impl *Impl) Request(req *ClientRequest, res *ClientResponse) {
	req.LogRequest()
	res.View = impl.viewManagerImpl.View
	res.RequestId = req.RequestId
	if !impl.viewManagerImpl.IsOpStatusNormal() {
		res.Result = 0
		res.Err = fmt.Errorf("invalid operation status of cluster: %s", impl.viewManagerImpl.GetLastOpStatus())
		return
	}
	if !impl.viewManagerImpl.IsPrimary() {
		res.Result = 0
		res.Err = fmt.Errorf("invalid operation status of host: %s", viewreplication.RoleBackup)
		return
	}

	clientState := impl.GetClientState(req.ClientId)
	if clientState == nil {
		impl.CreateClientState(req.ClientId)
	}
	lastResponse := clientState.LastResponse
	if req.RequestId < lastResponse.RequestId {
		res.Result = 0
		res.Err = fmt.Errorf("stale request id detected")
		return
	} else if req.RequestId == lastResponse.RequestId {
		res = clientState.LastResponse
		log.Printf("Request already executed; req: %v, res: %v", req, res)
		return
	}
	// Add request to queue to be processed in viewreplication package
	workReq := impl.BufferRequest(req)
	workReq.GetWG().Wait()
	*res = *(workReq.GetResult().(*ClientResponse))
}

func (impl *Impl) BufferRequest(req *ClientRequest) *clientrpc.WorkRequest {
	var wg sync.WaitGroup
	workReq := clientrpc.NewWorkRequest(req, &wg)
	impl.WorkQueue <- workReq
	wg.Add(1)
	return workReq
}

func (impl *Impl) GetClientState(clientId string) *ClientState {
	return impl.ClientStates[clientId]
}

func (impl *Impl) CreateClientState(clientId string) {
	impl.ClientStates[clientId] = &ClientState{
		Id:         clientId,
		LastResponse: &ClientResponse{
			View:      -1,
			RequestId: -1,
			Result:    -1,
			Err:       nil,
		},
	}
}

func (impl *Impl) ProcessClientRequests() {
	workReq := <-(impl.WorkQueue)
	req := workReq.GetRequest().(*ClientRequest)
	log.Printf("Received work request; req: %v", req)
	opId := impl.AdvanceOpId()
	op := impl.AppendRequest(req, opId)

	impl.UpdateClientState(workReq)
	done := impl.SendPrepareMessages(op)
	impl.WaitForQuorum(done)
	// TODO: Execute committed Operation
	// TODO: Set client response in work result
	// TODO: Update Client table
	workReq.GetWG().Done()
}

func (impl *Impl) AdvanceOpId() int {
	return impl.viewManagerImpl.AdvanceOpId()
}

func (impl *Impl) AppendRequest(req *ClientRequest, opId int) log2.Operation {
	logMsg := log2.LogMessage{
		ClientId:  req.ClientId,
		RequestId: req.RequestId,
		Log:       req.Op,
	}
	op := impl.viewManagerImpl.AppendOp(opId, logMsg)
	return op
}

func (impl *Impl) UpdateClientState(workReq *clientrpc.WorkRequest) {
	cr, ok := workReq.GetRequest().(*ClientRequest)
	if !ok {
		log.Fatalf("WorkRequest is not of type ClientRequest; req: %v", workReq)
	}
	cs := impl.ClientStates[cr.ClientId]
	cs.LastResponse = &ClientResponse{
		View:      impl.viewManagerImpl.View,
		RequestId: cr.RequestId,
		Result:    -1,
		Err:       nil,
	}
	workReq.SetResult(cs.LastResponse)
}

func (impl *Impl) SendPrepareMessages(op log2.Operation) chan *viewreplication.PrepareOkResponse {
	prepareReq := &viewreplication.PrepareRequest{
		View: impl.viewManagerImpl.View,
		OpId: op.OpId,
		Log: op.Log,
		CommitId: op.OpId,
	}
	done := make(chan *viewreplication.PrepareOkResponse, impl.viewManagerImpl.GetQuorumSize())
	impl.viewManagerImpl.SendPrepareMessages(prepareReq, done)
	return done
}

func (impl *Impl) WaitForQuorum(done chan *viewreplication.PrepareOkResponse) {
	var quorum int
	for quorum < impl.viewManagerImpl.GetQuorumSize() {
		res := <- done
		if res != nil {
			res.LogResponse()
			quorum += 1
		}
	}
}

func GetApp() *Impl {
	return appImpl
}

func Init() {
	viewmanager.Init()
	appImpl = &Impl{
		viewManagerImpl: viewmanager.GetImpl(),
		ClientStates:    make(map[string]*ClientState),
		WorkQueue:       make(chan *clientrpc.WorkRequest, maxRequests),
	}
	rpc.Register(appImpl)
	go appImpl.ProcessClientRequests()
}
