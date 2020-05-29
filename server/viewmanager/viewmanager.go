package viewmanager

import (
	"log"
	"net/rpc"
	"viewStampedReplication/server/clientrpc"
	log2 "viewStampedReplication/server/log"
	"viewStampedReplication/server/viewreplication"
)

var vmImpl *Impl

type ViewManager interface {
	StartView(req *StartViewRequest, res *clientrpc.EmptyResponse) error
	StartViewChange(req *StartViewChangeRequest, res *clientrpc.EmptyResponse) error
	DoViewChange(req *DoViewChangeRequest, res *clientrpc.EmptyResponse) error
	GetState(req *viewreplication.GetStateRequest, res *viewreplication.NewStateResponse) error
}

type Impl struct {
	*viewreplication.Impl
	ReplicaId                int
	pendingViewChangeRequest LockingRequest
	doViewChangeQueue chan *DoViewChangeRequest
}

func newVmImpl() *Impl {
	return &Impl{
		Impl: viewreplication.GetImpl(),
		pendingViewChangeRequest: LockingRequest{
			r:     nil,
		},
		doViewChangeQueue: make(chan *DoViewChangeRequest),
	}
}

func (impl *Impl) StartViewChange(req *StartViewChangeRequest, res *clientrpc.EmptyResponse) error {
	req.LogRequest()

	if req.View < impl.View {
		log.Printf("Current view is greater than view in request; current: %d, req: %d", impl.View, req.View)
		return nil
	}

	impl.pendingViewChangeRequest.Lock()
	defer impl.pendingViewChangeRequest.Unlock()

	if impl.pendingViewChangeRequest.r != nil {
		s := impl.pendingViewChangeRequest.r.(*StartViewChangeRequest)
		if req.View <= s.View {
			log.Printf("Current view change request is not greater than view in current request; pending: %d, req: %d", req.View, s.View)
			return nil
		}
		log.Printf("New view is greater than pending view change; pending: %d, req: %d", s.View, req.View)
	}

	impl.pendingViewChangeRequest.r = req

	impl.SetClusterStatusViewChange()

	for _, replica := range impl.GetOtherReplicas() {
		if req.ReplicaId == replica.Id {
			doViewChangeReq := &DoViewChangeRequest{
				View:             req.View,
				Ops:              impl.Ops,
				LatestNormalView: impl.View,
				OpId:             impl.OpId,
				CommitId:         impl.CommitId,
			}
			replica.Do("ViewManager.DoViewChange", doViewChangeReq, &clientrpc.EmptyResponse{}, false)
		}
	}
	return nil
}

func (impl *Impl) DoViewChange(req *DoViewChangeRequest, res *clientrpc.EmptyResponse) error {
	req.LogRequest()
	if impl.pendingViewChangeRequest.r != nil {
		pendingReq := impl.pendingViewChangeRequest.r.(*StartViewChangeRequest)
		if pendingReq.View <= req.View {
			impl.doViewChangeQueue <- req
			return nil
		}
		log.Printf("Pending view change is for a greater view. Ignoring DoViewChange request; pendingView: %d, reqView: %d", pendingReq.View, req.View)
		return nil
	}
	impl.SetClusterStatusViewChange()
	impl.doViewChangeQueue <- req
	return nil
}

func (impl *Impl) ProcessDoViewChangeRequests() {
	var quorum int
	var maxLatestNormalView = -1
	var maxOpId = -1
	var maxCommitId = -1
	var opsFromLatestNormalView = make([]log2.Operation, 0)
	for {
		req := <- impl.doViewChangeQueue
		quorum += 1

		if req.LatestNormalView > maxLatestNormalView {
			maxLatestNormalView = req.LatestNormalView
			opsFromLatestNormalView = req.Ops
			maxOpId = req.OpId
		} else if req.LatestNormalView == maxLatestNormalView {
			if req.OpId > maxOpId {
				maxOpId = req.OpId
				opsFromLatestNormalView = req.Ops
			}
		}
		if req.CommitId > maxCommitId {
			maxCommitId = req.CommitId
		}
		if quorum == impl.GetQuorumSize() {
			impl.View = req.View
			impl.Ops = opsFromLatestNormalView
			impl.OpId = impl.Ops[len(impl.Ops)-1].OpId
			impl.CommitId = maxCommitId
			// Reset local state.
			quorum = 0
			maxLatestNormalView = -1
			maxOpId = -1
			maxCommitId = -1
			opsFromLatestNormalView = make([]log2.Operation, 0)
			impl.UpdatePrimaryNode(impl.Config.Id)
			impl.SetClusterStatusNormal()
			// Clear pending view change request as its done.
			impl.pendingViewChangeRequest.Lock()
			impl.pendingViewChangeRequest.r = nil
			impl.pendingViewChangeRequest.Unlock()
			// Send StartView requests to replicas.
			startViewReq := &StartViewRequest{
				View:     impl.View,
				Ops:      impl.Ops,
				OpId:     impl.OpId,
				CommitId: impl.CommitId,
			}
			for _, replica := range impl.GetOtherReplicas() {
				replica.Do("ViewManager.StartView", startViewReq, &clientrpc.EmptyResponse{}, true)
			}
		}
	}
}

func (impl *Impl) StartView(req *StartViewRequest, res *clientrpc.EmptyResponse) error {
	req.LogRequest()
	impl.View = req.View
	impl.Ops = req.Ops
	impl.OpId = req.OpId
	impl.SetClusterStatusNormal()
	lastOp := impl.Ops[len(impl.Ops)-1]
	impl.UpdateClientState(lastOp.Log.ClientId, lastOp.Log.RequestId, lastOp.Result)
	for _, op := range impl.Ops {
		if op.Result == nil {
			// send PrepareOk messages for non-committed ops.
			req := &viewreplication.PrepareOkRequest{
				View:      impl.View,
				OpId:      op.OpId,
				ReplicaId: impl.Config.Id,
			}
			impl.GetPrimary().Do("ViewStampedReplication.PrepareOk", req, &clientrpc.EmptyResponse{}, true)
		}
	}
	return nil
}

func (impl *Impl) GetState(req *viewreplication.GetStateRequest, res *viewreplication.NewStateResponse) error {
	if !impl.IsClusterStatusNormal() {
		log.Printf("Ignoring GetStateRequest due to invalid cluster status; status: %s", impl.ClusterStatus)
		return nil
	}
	impl.Impl.GetState(req, res)
	return nil
}

func (impl *Impl) SendPrepareMessages(clientId string, requestId int, op log2.Operation) chan *viewreplication.PrepareOkResponse {
	impl.ActivityTimer.Reset(impl.ActivityTimeout)
	req := &viewreplication.PrepareRequest{
	 	ClientId: clientId,
	 	RequestId: requestId,
		View: impl.View,
		OpId: op.OpId,
		Log: op.Log,
		CommitId: op.OpId,
	}
	done := make(chan *viewreplication.PrepareOkResponse, impl.GetQuorumSize())
	backups := impl.GetBackups()
	for _, backup := range backups {
		go func(backup *viewreplication.Replica) {
			var res *viewreplication.PrepareOkResponse
			log.Printf("Sending Prepare request to backup; backup: %d, req: %v", backup.Id, req)
			backup.Do("ViewReplication.Prepare", req, res, false)
			done <- res
		}(backup)
	}
	return done
}

func (impl *Impl) AdvanceView() {
	impl.View += 1
}

func (impl *Impl) RequestViewChange() {
	impl.ClusterStatus = viewreplication.StatusViewChange
	impl.AdvanceView()
	impl.SendStartViewChangeRequests()
}

func (impl *Impl) SendStartViewChangeRequests() {
	req := &StartViewChangeRequest{
		ReplicaId: impl.Config.Id,
		View:      impl.View,
	}
	for _, replica := range impl.GetOtherReplicas() {
		log.Printf("Sending StartViewChange request to replica; replica: %d, req: %v", replica.Id, req)
		replica.Do("ViewManager.StartViewChange", req, nil, false)
	}
}

func (impl *Impl) MonitorActivity() {
	for {
		<- impl.ActivityTimer.C
		impl.ActivityTimer.Reset(impl.ActivityTimeout)
		if impl.IsPrimary() {
			if !impl.IsClusterStatusNormal() {
				continue
			}
			// After timeout, send commits as primary.
			for _, replica := range impl.GetBackups() {
				commitReq := &viewreplication.CommitRequest{
					View:     impl.View,
					CommitId: impl.CommitId,
				}
				log.Printf("[INFO] Sending commit request to replica; replica: %d, req: %v", replica.Id, commitReq)
				replica.Do("ViewReplication.Commit", commitReq, nil, true)
			}
		} else {
			// After timeout, request view change.
			impl.RequestViewChange()
		}
	}
}

func GetImpl() *Impl {
	return vmImpl
}

func Init() {
	viewreplication.Init()
	vmImpl = newVmImpl()
	rpc.RegisterName("ViewManager", vmImpl)
	log.Print("ViewManager initialization successful")
	go vmImpl.MonitorActivity()
}