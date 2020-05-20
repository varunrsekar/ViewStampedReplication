package viewreplication

import (
	"fmt"
	"log"
	"net/rpc"
	"sync"
	"viewStampedReplication/server/clientrpc"
	log2 "viewStampedReplication/server/log"
	"viewStampedReplication/server/serviceconfig"
)

const (
	maxRequests = 100
)
var vrImpl *Impl

func NewReplica(id int, client clientrpc.Client, port int, role Role) Replica {
	return Replica{
		Id: id,
		c: client,
		port: port,
		role: role,
	}
}

func(r *Replica) Do(api string, req clientrpc.Request, res clientrpc.Response, async bool) {
	if async {
		r.c.AsyncDo(api, req, res)
	} else {
		r.c.Do(api, req, res)
	}
}

func (impl *Impl) Prepare(req *PrepareRequest, res *PrepareOkResponse) {
	workReq := impl.BufferPrepareRequest(req)
	workReq.GetWG().Wait()
	*res = *(workReq.GetResult().(*PrepareOkResponse))
}

func (impl *Impl) BufferPrepareRequest(req *PrepareRequest) *clientrpc.WorkRequest {
	var wg sync.WaitGroup
	workReq := clientrpc.NewWorkRequest(req, &wg)
	impl.PrepareQueue <- workReq
	wg.Add(1)
	return workReq
}

func (impl *Impl) ProcessPrepareRequests() {
	workReq := <-(impl.PrepareQueue)
	req := workReq.GetRequest().(*PrepareRequest)
	currOpId := impl.OpId
	if req.OpId <= currOpId {
		log.Fatalf("Received invalid OpId in request; req: %v", req)
	} else if req.OpId == currOpId + 1 {

	}
	workReq.GetWG().Done()
}

func (impl *Impl) ProcessCommitRequests() {
	workReq := <-(impl.CommitQueue)
	req := workReq.GetRequest().(*CommitRequest)
	// TODO: Process CommitRequest here
	workReq.GetWG().Done()
}

func (impl *Impl) BufferCommitRequest(req *CommitRequest) *clientrpc.WorkRequest {
	var wg sync.WaitGroup
	workReq := clientrpc.NewWorkRequest(req, &wg)
	impl.CommitQueue <- workReq
	wg.Add(1)
	return workReq
}

func (impl *Impl) AdvanceOpId() int {
	impl.OpId += 1
	return impl.OpId
}

func (impl *Impl) AppendOp(opId int, logMsg log2.LogMessage) log2.Operation {
	op := log2.Operation{
		OpId: opId,
		Log: logMsg,
	}
	impl.Ops = append(impl.Ops, op)
	return op
}

func (impl *Impl) GetBackups() []Replica {
	backups := make([]Replica, 0)
	for _, replica := range impl.Config.replicas {
		if !replica.IsPrimary() {
			backups = append(backups, replica)
		}
	}
	return backups
}

func (impl *Impl) GetPrimary() *Replica {
	for _, replica := range impl.Config.replicas {
		if replica.IsPrimary() {
			return &replica
		}
	}
	return nil
}

func (impl *Impl) GetOtherReplicas() []Replica {
	replicas := make([]Replica, 0)
	for _, replica := range impl.Config.replicas {
		if impl.Config.Id == replica.Id {
			replicas = append(replicas, replica)
		}
	}
	return replicas
}

func (impl *Impl) IsPrimary() bool {
	return impl.Config.IsPrimary()
}

func(impl *Impl) IsOpStatusNormal() bool {
	return impl.OpStatus == StatusNormal
}

func (impl *Impl) GetQuorumSize() int {
	return impl.Config.QuorumSize
}

func GetImpl() *Impl {
	return vrImpl
}

func prepareConfig() Configuration {
	sConfig := serviceconfig.GetConfig()
	var role Role
	var isPrimary bool
	replicas := make(map[int]Replica)
	for _, replica := range sConfig.Replicas {
		if replica.Primary {
			role = RolePrimary
			if sConfig.Id == replica.Id {
				isPrimary = true
			}
		} else {
			role = RoleBackup
		}
		c := clientrpc.Client{
			Hostname: fmt.Sprintf("localhost:%d", replica.Port),
		}
		replicas[replica.Id] = NewReplica(replica.Id, c, replica.Port, role)
	}

	return Configuration{
		Id:       sConfig.Id,
		QuorumSize: sConfig.QuorumSize,
		replicas: replicas,
		primary:  isPrimary,
	}
}
func Init() {
	config := prepareConfig()
	vrImpl = &Impl{
		Config: config,
		PrepareQueue: make(chan *clientrpc.WorkRequest, maxRequests),
		CommitQueue: make(chan *clientrpc.WorkRequest, maxRequests),
	}
	rpc.Register(vrImpl)
	if !config.IsPrimary() {
		go vrImpl.ProcessPrepareRequests()
		go vrImpl.ProcessCommitRequests()
	}
}