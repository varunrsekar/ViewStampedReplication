package viewmanager

import (
	"log"
	"net/rpc"
	log2 "viewStampedReplication/server/log"
	"viewStampedReplication/server/viewreplication"
)

var vmImpl *Impl

func newVmImpl() *Impl {
	return &Impl{
		vrImpl: viewreplication.GetImpl(),
	}
}

func (impl *Impl) StartViewChange(req *StartViewChangeRequest) {
	if req.View <= impl.View {
		log.Printf("Current viewreplication is greater than viewreplication in request; current: %d, req: %d", impl.View, req.View)
	}
	impl.pendingViewChangeRequest.Lock()
	defer impl.pendingViewChangeRequest.Unlock()
	s := impl.pendingViewChangeRequest.r.(*StartViewChangeRequest)
	if req.View > s.View {
		log.Printf("New viewreplication is greater than pending viewreplication change; pending: %d, req: %d", s.View, req.View)
		newReq := &StartViewChangeRequest{
			View: req.View,
			ReplicaId: impl.ReplicaId,
		}
		impl.pendingViewChangeRequest.r = newReq
		for _, replica := range impl.vrImpl.GetOtherReplicas() {
			replica.Do("ViewManager.StartViewChange", newReq, nil, true)
		}
	}
}

func (impl *Impl) AppendOp(opId int, logMsg log2.LogMessage) log2.Operation {
	return impl.vrImpl.AppendOp(opId, logMsg)
}

func (impl *Impl) SendPrepareMessages(req *viewreplication.PrepareRequest, done chan *viewreplication.PrepareOkResponse) {
	backups := impl.vrImpl.GetBackups()
	for _, backup := range backups {
		go func(backup *viewreplication.Replica) {
			var res *viewreplication.PrepareOkResponse
			backup.Do("ViewReplication.Prepare", req, res, false)
			done <- res
		}(&backup)
	}
}

func (impl *Impl) AdvanceOpId() int {
	return impl.vrImpl.AdvanceOpId()
}

func (impl *Impl) IsPrimary() bool {
	return impl.vrImpl.IsPrimary()
}

func(impl *Impl) IsOpStatusNormal() bool {
	return impl.vrImpl.IsOpStatusNormal()
}

func (impl *Impl) GetLastOpStatus() viewreplication.Status {
	return impl.vrImpl.OpStatus
}

func (impl *Impl) GetQuorumSize() int {
	return impl.vrImpl.GetQuorumSize()
}

func GetImpl() *Impl {
	return vmImpl
}

func Init() {
	viewreplication.Init()
	vmImpl = newVmImpl()
	rpc.Register(vmImpl)
}