package viewmanager

import (
	"github.com/davecgh/go-spew/spew"
	"log"
	"net/rpc"
	"time"
	"viewStampedReplication/clientrpc"
	log2 "viewStampedReplication/log"
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
	pendingViewChangeRequests LockingRequests
	doViewChangeQueue chan *DoViewChangeRequest
	startViewChangeQueue chan *StartViewChangeRequest
	LatestNormalView int
	sentDoViewChange map[int]bool
	sentStartViewChange map[int]bool
}

func newVmImpl() *Impl {
	return &Impl{
		Impl: viewreplication.GetImpl(),
		pendingViewChangeRequests: LockingRequests{
			r: make(map[int]clientrpc.Request),
		},
		doViewChangeQueue: make(chan *DoViewChangeRequest),
		startViewChangeQueue: make(chan *StartViewChangeRequest),
		sentDoViewChange: make(map[int]bool),
		sentStartViewChange: make(map[int]bool),
	}
}

func (impl *Impl) StartViewChange(req *StartViewChangeRequest, res *clientrpc.EmptyResponse) error {
		req.LogRequest(true)

		//impl.pendingViewChangeRequests.Lock()
		var quorum int
		quorum = len(impl.pendingViewChangeRequests.r)
		log.Printf("Quorum: %d", quorum)

		if req.View < impl.View {
			log.Printf("Current view is greater than view in request; current: %d, req: %d", impl.View, req.View)
			//impl.pendingViewChangeRequests.Unlock()
			return nil
		} else if req.View > impl.View {
			log.Printf("Requested StartViewChange is for a greater view; Initiating a new view change request; reqView: %d, currView: %d", req.View, impl.View)
			impl.pendingViewChangeRequests.r = make(map[int]clientrpc.Request)
			impl.pendingViewChangeRequests.r[req.ReplicaId] = req
			impl.RequestViewChange(req.View)
			//impl.pendingViewChangeRequests.Unlock()
			return nil
		}

		if impl.pendingViewChangeRequests.r[req.ReplicaId] != nil {
			s := impl.pendingViewChangeRequests.r[req.ReplicaId].(*StartViewChangeRequest)
			if req.View < s.View {
				log.Printf("Current view change request is not greater than view in current request; pending: %d, req: %d", req.View, s.View)
				//impl.pendingViewChangeRequests.Unlock()
				return nil
			}
			log.Printf("New view is greater than pending view change; pending: %d, req: %d", s.View, req.View)
		}

		log.Printf("Adding StartViewChange request from replica %d to self; req: %+v", req.ReplicaId, req)
		impl.pendingViewChangeRequests.r[req.ReplicaId] = req

		if quorum < impl.GetQuorumSize() {
			log.Printf("Quorum not reached; reqCount: %d", quorum)
			selfReq := impl.pendingViewChangeRequests.r[impl.Config.Id]
			if  selfReq != nil {
				// Send StartViewChange requests if I haven't already.
				impl.RequestViewChange(req.View)
			}
			return nil
		}
		impl.SendDoViewChangeRequest()
		//impl.pendingViewChangeRequests.Unlock()
		return nil
}

func (impl *Impl) SendDoViewChangeRequest() {
	if impl.sentDoViewChange[impl.View] {
		return
	}
	newPrimary := impl.findNewPrimary()
	replicas := impl.GetOtherReplicas()
	for i, _:= range replicas {
		if replicas[i].Id == newPrimary {
			doViewChangeReq := &DoViewChangeRequest{
				View:             impl.View,
				Ops:              impl.Ops,
				LatestNormalView: impl.LatestNormalView,
				OpId:             impl.OpId,
				CommitId:         impl.CommitId,
				ReplicaId:        impl.Config.Id,
			}
			doViewChangeReq.LogRequest(false)
			log.Printf("Sending DoViewChangeRequest to replica; replica: %d, req: %+v", replicas[i].Id, doViewChangeReq)
			replicas[i].Do("ViewManager.DoViewChange", doViewChangeReq, &clientrpc.EmptyResponse{}, false)
			break
		}
	}
	impl.sentDoViewChange[impl.View] = true
}

func (impl *Impl) findNewPrimary() int {
	currPrimary := impl.GetPrimary()
	newPrimary := currPrimary.Id + 1
	if newPrimary == len(impl.GetOtherReplicas())+1 {
		newPrimary = 0
	}
	log.Printf("New primary is replica %d", newPrimary)
	return newPrimary
}

func (impl *Impl) DoViewChange(req *DoViewChangeRequest, res *clientrpc.EmptyResponse) error {
	req.LogRequest(true)

	if req.View > impl.View {
		log.Printf("Requested DoViewChange is for a greater view; Initiating a new view change request; reqView: %d, currView: %d", req.View, impl.View)
		impl.RequestViewChange(req.View)
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
	var currView = -1
	var opsFromLatestNormalView = make([]log2.Operation, 0)
	for {
		req := <- impl.doViewChangeQueue
		quorum += 1
		if req.View > currView {
			quorum = 1
			maxOpId = req.OpId
			maxLatestNormalView = req.LatestNormalView
			maxCommitId = req.CommitId
			currView = req.View
			opsFromLatestNormalView = req.Ops
			continue
		}
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
			if len(impl.Ops) > 0 {
				impl.OpId = impl.Ops[len(impl.Ops)-1].OpId
			}
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
			impl.pendingViewChangeRequests.r = make(map[int]clientrpc.Request)
			// Send StartView requests to replicas.
			startViewReq := &StartViewRequest{
				View:     impl.View,
				Ops:      impl.Ops,
				OpId:     impl.OpId,
				CommitId: impl.CommitId,
				ReplicaId: impl.Config.Id,
			}
			startViewReq.LogRequest(false)
			replicas := impl.GetOtherReplicas()
			for i, _ := range replicas {
				replicas[i].Do("ViewManager.StartView", startViewReq, &clientrpc.EmptyResponse{}, true)
			}
			// Start sending commit requests after half the time.
			impl.ActivityTimer.Reset(impl.ActivityTimeout/2)
		}
	}
}

func (impl *Impl) StartView(req *StartViewRequest, res *clientrpc.EmptyResponse) error {
	req.LogRequest(true)

	impl.View = req.View
	impl.Ops = req.Ops
	impl.OpId = req.OpId
	//impl.pendingViewChangeRequests.Lock()
	//defer impl.pendingViewChangeRequests.Unlock()
	impl.sentStartViewChange = make(map[int]bool)
	impl.sentDoViewChange = make(map[int]bool)
	impl.pendingViewChangeRequests.r = make(map[int]clientrpc.Request)
	impl.UpdatePrimaryNode(req.ReplicaId)
	impl.SetPrimaryReqRecvd(false)
	impl.ActivityTimer.Reset(impl.ActivityTimeout)
	impl.SetClusterStatusNormal()
	if len(impl.Ops) == 0 {
		return nil
	}
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
			req.LogRequest(false)
			replica := impl.GetPrimary()
			(&replica).Do("Application.PrepareOk", req, &clientrpc.EmptyResponse{}, true)
		}
	}
	impl.ActivityTimer.Reset(impl.ActivityTimeout)
	return nil
}

func (impl *Impl) GetState(req *viewreplication.GetStateRequest, res *viewreplication.NewStateResponse) error {
	if !impl.IsClusterStatusNormal() {
		*res = viewreplication.NewStateResponse{
			View:      -1,
			OpId:      -1,
			CommitId:  -1,
			Ops:       make([]log2.Operation, 0),
			ReplicaId: impl.Config.Id,
		}
		log.Printf("Ignoring GetStateRequest due to invalid cluster status; status: %s", impl.ClusterStatus)
		return nil
	}
	vrRes := impl.Impl.GetState(req)
	if vrRes != nil {
		*res = viewreplication.NewStateResponse{
			View:      vrRes.View,
			OpId:      vrRes.OpId,
			CommitId:  vrRes.CommitId,
			Ops:       vrRes.Ops,
			ReplicaId: vrRes.ReplicaId,
		}
	}
	res.LogResponse(false)
	return nil
}

func (impl *Impl) SendPrepareMessages(clientId string, requestId int, op log2.Operation) {
	impl.ActivityTimer.Reset(impl.ActivityTimeout)
	req := &viewreplication.PrepareRequest{
	 	ClientId: clientId,
	 	RequestId: requestId,
		View: impl.View,
		OpId: op.OpId,
		Log: op.Log,
		CommitId: op.OpId,
		ReplicaId: impl.Config.Id,
	}
	req.LogRequest(false)
	backups := impl.GetOtherReplicas()
	log.Printf("backups: %+v", spew.NewFormatter(backups))
	for i, _ := range backups {
		backups[i].Do("ViewReplication.Prepare", req, &clientrpc.EmptyResponse{}, true)
	}
}

func (impl *Impl) AdvanceView() {
	if impl.IsClusterStatusNormal() {
		impl.LatestNormalView = impl.View
	}
	impl.View += 1
}

func (impl *Impl) RequestViewChange(view int) {
	for impl.View < view {
		impl.AdvanceView()
	}
	if impl.sentDoViewChange[impl.View] || impl.sentStartViewChange[impl.View] {
		return
	}
	impl.SetClusterStatusViewChange()
	impl.SendStartViewChangeRequests()
}

func (impl *Impl) SendStartViewChangeRequests() {
	if impl.sentStartViewChange[impl.View] {
		return
	}
	req := &StartViewChangeRequest{
		ReplicaId: impl.Config.Id,
		View:      impl.View,
	}
	if impl.pendingViewChangeRequests.r[impl.Config.Id] != nil && impl.pendingViewChangeRequests.r[impl.Config.Id].(*StartViewChangeRequest).View == impl.View {
		return
	}
	impl.pendingViewChangeRequests.r[impl.Config.Id] = req
	replicas := impl.GetOtherReplicas()
	for i, _ := range replicas {
		log.Printf("Sending StartViewChange request to replica; replica: %d, req: %v", replicas[i].Id, req)
		req.LogRequest(false)
		replicas[i].Do("ViewManager.StartViewChange", req, &clientrpc.EmptyResponse{}, true)
	}
	impl.sentStartViewChange[impl.View] = true
}

func (impl *Impl) MonitorActivity() {
	if impl.IsPrimary() {
		impl.WaitForCluster()
	}
	for {
		t := <- impl.ActivityTimer.C
		log.Printf("Hit Activity timer at %v", t.Format(time.RFC3339))
		impl.ActivityTimer.Reset(impl.ActivityTimeout)
		if impl.IsPrimary() {
			if !impl.IsClusterStatusNormal() {
				continue
			}
			// After timeout, send commits as primary.
			backups := impl.GetOtherReplicas()
			for i, _ := range backups {
				commitReq := &viewreplication.CommitRequest{
					View:     impl.View,
					CommitId: impl.CommitId,
					ReplicaId: impl.Config.Id,
				}
				commitReq.LogRequest(false)
				backups[i].Do("ViewReplication.Commit", commitReq, &clientrpc.EmptyResponse{}, true)
			}
		} else {
			if !impl.IsClusterStatusNormal() {
				continue
			}
			// If never received first request from primary.
			if !impl.IsPrimaryReqRecvd() {
				continue
			}
			// After timeout, request view change.
			impl.RequestViewChange(impl.View+1)
		}
	}
}

func (impl *Impl) WaitForCluster() {
	for {
		var count int
		var err error
		backups := impl.GetBackups()
		for i, _ := range backups {
			commitReq := &viewreplication.CommitRequest{
				View:     impl.View,
				CommitId: impl.CommitId,
				ReplicaId: impl.Config.Id,
			}
			commitReq.LogRequest(false)
			err = backups[i].Do("ViewReplication.Commit", commitReq, nil, true)
			if err != nil {
				continue
			}
			count++
		}
		if count != len(impl.GetOtherReplicas()) {
			time.Sleep(time.Second)
			continue
		}
		log.Print("All replicas are online; Cluster is formed")
		break
	}
}

func GetImpl() *Impl {
	return vmImpl
}

func Init() {
	viewreplication.Init()
	vmImpl = newVmImpl()
	rpc.RegisterName("ViewManager", vmImpl)
	go vmImpl.ProcessDoViewChangeRequests()
	go vmImpl.MonitorActivity()
	log.Print("ViewManager initialization successful")
}