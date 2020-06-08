package viewreplication

import (
	"bytes"
	"fmt"
	"github.com/davecgh/go-spew/spew"
	"log"
	"math/rand"
	"net/rpc"
	"strconv"
	"sync"
	"time"
	"viewStampedReplication/clientrpc"
	log2 "viewStampedReplication/log"
	"viewStampedReplication/server/serviceconfig"
)

const (
	primaryTimeout = 4800 // Timeout for primary node in milliseconds.
	backupTimeout = 5800 // Timeout for backup node in milliseconds.
)

var vrImpl *Impl

// Protocol starts only after receiving commit from primary.
var isPrimaryReqRecvd bool

type ViewReplication interface {
	Prepare(req *PrepareRequest, res *clientrpc.EmptyResponse) error
	Commit(req *CommitRequest, res *clientrpc.EmptyResponse) error
	GetState(req *GetStateRequest, res *NewStateResponse) error
	Recovery(req *RecoveryRequest, res *RecoveryResponse) error
}

type ClientState struct {
	Id string
	RequestId int
	LastResponse *OpResponse
}

type Impl struct {
	Config          Configuration
	View            int
	OpId            int
	Ops             []log2.Operation
	CommitId        int
	ClusterStatus   Status
	ClientStates    map[string]*ClientState
	PrepareQueue    chan *clientrpc.WorkRequest
	CommitQueue     chan *clientrpc.WorkRequest
	ActivityTimer   *time.Timer
	ActivityTimeout time.Duration
}

func (impl *Impl) Prepare(req *PrepareRequest, res *clientrpc.EmptyResponse) error {
	req.LogRequest(true)
	impl.SetPrimaryReqRecvd(true)
	workReq := impl.BufferPrepareRequest(req)
	workReq.GetWG().Wait()
	if workReq.GetResult() != nil {
		return nil
	}
	return fmt.Errorf("prepare failed for req %v", req)
}

func (impl *Impl) BufferPrepareRequest(req *PrepareRequest) *clientrpc.WorkRequest {
	impl.ActivityTimer.Reset(impl.ActivityTimeout)

	var wg sync.WaitGroup
	workReq := clientrpc.NewWorkRequest(req, &wg)
	impl.PrepareQueue <- workReq
	wg.Add(1)
	return workReq
}

func (impl *Impl) ProcessPrepareRequests() {
	for {
		workReq := <-(impl.PrepareQueue)
		if impl.IsPrimary() {
			workReq.SetResult(nil)
			workReq.GetWG().Done()
			continue
		}
		req := workReq.GetRequest().(*PrepareRequest)
		currOpId := impl.OpId
		if req.OpId <= currOpId {
			log.Printf("[error] Received invalid OpId in request; req: %v", req)
			workReq.SetResult(nil)
			workReq.GetWG().Done()
			continue
		} else if req.OpId > currOpId+1 {
			impl.WaitForStateTransfer(req.View)
		}
		if req.CommitId > impl.CommitId {
			impl.DoPendingCommits(req.CommitId)
		}
		impl.AdvanceOpId()
		impl.AppendOp(req.OpId, req.Log)
		impl.UpdateClientState(req.ClientId, req.RequestId, nil)
		primary := impl.GetPrimary()
		okReq := &PrepareOkRequest{
			View:      impl.View,
			OpId:      req.OpId,
			ReplicaId: impl.Config.Id,
			DestId: primary.Id,
		}
		okReq.LogRequest(false)
		workReq.SetResult(&clientrpc.EmptyResponse{})
		(&primary).Do("Application.PrepareOk", okReq, &clientrpc.EmptyResponse{}, true)
		workReq.GetWG().Done()
	}
}

func (impl *Impl) Commit(req *CommitRequest, res *clientrpc.EmptyResponse) error {
	req.LogRequest(true)
	impl.ActivityTimer.Reset(impl.ActivityTimeout)
	isPrimaryReqRecvd = true
	workReq := impl.BufferCommitRequest(req)
	workReq.GetWG().Wait()
	return nil
}

func (impl *Impl) BufferCommitRequest(req *CommitRequest) *clientrpc.WorkRequest {
	var wg sync.WaitGroup
	workReq := clientrpc.NewWorkRequest(req, &wg)
	impl.CommitQueue <- workReq
	wg.Add(1)
	return workReq
}

func (impl *Impl) ProcessCommitRequests() {
	for {
		workReq := <-(impl.CommitQueue)

		req := workReq.GetRequest().(*CommitRequest)
		currOpId := impl.OpId
		// If node is lagging behind, do state transfer.
		if req.CommitId > currOpId || req.View > impl.View {
			impl.WaitForStateTransfer(req.View)
		}
		if req.CommitId == impl.CommitId {
			workReq.GetWG().Done()
			continue
		}
		for i, _ := range impl.Ops {
			if req.CommitId == impl.Ops[i].OpId {
				if impl.Ops[i].Committed {
					break
				}
				result := impl.Ops[i].Commit()
				impl.CommitId = req.CommitId
				impl.UpdateClientState(impl.Ops[i].Log.ClientId, impl.Ops[i].Log.RequestId, result)
			}
		}
		workReq.GetWG().Done()
	}
}

func (impl *Impl) GetState(req *GetStateRequest) *NewStateResponse {
	req.LogRequest(true)

	if req.View > impl.View {
		return nil
	}

	ops := make([]log2.Operation, 0)
	for _, op := range impl.Ops {
		if op.OpId <= req.OpId {
			continue
		}
		ops = append(ops, op)
	}
	res := &NewStateResponse{
		View:     impl.View,
		OpId:     impl.OpId,
		CommitId: impl.CommitId,
		Ops:      ops,
		ReplicaId: impl.Config.Id,
		DestId: req.ReplicaId,
	}
	return res
}

func (impl *Impl) WaitForStateTransfer(view int) {
	if view >= impl.View {
		// Set opID to last committed op and clear all ops after it.
		impl.OpId = impl.CommitId
		impl.ClearOpsAfterOpId()
		replicas := impl.GetOtherReplicas()
		for i, _ := range replicas {
			var res = NewStateResponse{
				View:      view,
				OpId:      -2,
				CommitId:  -2,
				Ops:       nil,
				Primary: -1,
				ReplicaId: replicas[i].Id,
				DestId: impl.Config.Id,
			}
			var opId int
			if view == impl.View {
				opId = impl.OpId
			} else {
				opId = impl.CommitId
			}
			getStateReq := GetStateRequest{
				View:      view,
				OpId:      opId,
				ReplicaId: impl.Config.Id,
				DestId: replicas[i].Id,
			}
			getStateReq.LogRequest(false)
				replicas[i].Do("ViewManager.GetState", &getStateReq, &res, false)
				res.LogResponse(true)
				if res.View == -1 || res.OpId < opId {
					// This means the replica hasn't processed the state transfer request and is ignoring it.
					continue
				}
				for _, op := range res.Ops {
					impl.Ops = append(impl.Ops, op)
				}
				// Update latest primary node after state transfer.
				impl.UpdatePrimaryNode(res.Primary)
				impl.View = res.View
				impl.OpId = res.OpId
				impl.DoPendingCommits(res.CommitId)
				break
		}
	}
}

func (impl *Impl) DoPendingCommits(commitId int) {
	for i, _  := range impl.Ops {
		if impl.Ops[i].OpId <= impl.CommitId || impl.Ops[i].OpId > commitId {
			continue
		}
		impl.Ops[i].Committed = false
		result := impl.Ops[i].Commit()
		impl.CommitId = impl.Ops[i].OpId
		impl.UpdateClientState(impl.Ops[i].Log.ClientId, impl.Ops[i].Log.RequestId, result)
	}
}

func (impl *Impl) ClearOpsAfterOpId() {
	var idx = 0
	for _, op := range impl.Ops {
		if op.OpId <= impl.OpId {
			idx++
		}
	}
	// If no logs are present, do nothing.
	if idx == 0 {
		return
	}
	impl.Ops = impl.Ops[:idx-1]
}

func (impl *Impl) AdvanceOpId() int {
	impl.OpId += 1
	return impl.OpId
}

func (impl *Impl) AppendOp(opId int, logMsg log2.LogMessage) log2.Operation {
	op := log2.Operation{
		OpId: opId,
		Log: logMsg,
		Quorum: make(map[int]bool),
		Committed: false,
	}
	impl.Ops = append(impl.Ops, op)
	return op
}

func (impl *Impl) GetBackups() []Replica {
	backups := make([]Replica, 0)
	for i, replica := range impl.Config.Replicas {
		if !replica.IsPrimary() {
			backups = append(backups, impl.Config.Replicas[i])
		}
	}
	return backups
}

func (impl *Impl) GetPrimary() Replica {
	for i, replica := range impl.Config.Replicas {
		if replica.IsPrimary() {
			return impl.Config.Replicas[i]
		}
	}
	return Replica{}
}

func (impl *Impl) GetOtherReplicas() []Replica {
	replicas := make([]Replica, 0)
	for i, _ := range impl.Config.Replicas {
		if impl.Config.Id != impl.Config.Replicas[i].Id {
			replicas = append(replicas, impl.Config.Replicas[i])
		}
	}
	var selfIdx = -1
	for i, replica := range replicas {
		if replica.Id == impl.Config.Id {
			selfIdx = i
			break
		}
	}
	if selfIdx != -1 {
		replicas[selfIdx] = replicas[len(replicas)-1]
		replicas = replicas[:len(replicas)-1]
	}
	return replicas
}

func (impl *Impl) IsPrimary() bool {
	return impl.Config.IsPrimary()
}

func (impl *Impl) GetQuorumSize() int {
	return impl.Config.QuorumSize
}

func (impl *Impl) GetClientState(clientId string) *ClientState {
	return impl.ClientStates[clientId]
}

func (impl *Impl) CreateClientState(clientId string) *ClientState {
	impl.ClientStates[clientId] = &ClientState{
		Id:         clientId,
	}
	return impl.ClientStates[clientId]
}

func (impl *Impl) UpdateClientState(clientId string, requestId int, res *log2.OpResult) *OpResponse {
	cs := impl.ClientStates[clientId]
	if cs == nil {
		log.Printf("Failed to lookup clientId %s; Creating new client state", clientId)
		cs = impl.CreateClientState(clientId)
	}
	if requestId > cs.RequestId {
		cs.RequestId = requestId
	}
	if res != nil {
		cs.LastResponse = &OpResponse{
			View:      impl.View,
			RequestId: requestId,
			Result:    res,
		}
	}
	return cs.LastResponse
}

func (impl *Impl) Recovery(req *RecoveryRequest, res *RecoveryResponse) error {
	req.LogRequest(true)
	if !impl.IsClusterStatusNormal() {
		log.Printf("Cluster status is not normal. Ignoring request; status: %s, req: %+v", impl.ClusterStatus, req)
		return nil
	}
	if !impl.IsPrimary() {
		log.Printf("Returning RecoveryResponse as backup; replica: %d, reqNonce: %v", impl.Config.Id, req.Nonce)
		*res = RecoveryResponse{
			View:      impl.View,
			Ops:       make([]log2.Operation, 0),
			Nonce:     req.Nonce,
			OpId:      -1,
			CommitId:  -2,
			ReplicaId: impl.Config.Id,
			DestId: req.ReplicaId,
		}
		res.LogResponse(false)
		return nil
	}
	log.Printf("Returning RecoveryResponse as primary; replica: %d, reqNonce: %v", impl.Config.Id, req.Nonce)
	*res = RecoveryResponse{
		View:      impl.View,
		Ops:       impl.Ops,
		Nonce:     req.Nonce,
		OpId:      impl.OpId,
		CommitId:  impl.CommitId,
		ReplicaId: impl.Config.Id,
		DestId: req.ReplicaId,
	}
	res.LogResponse(false)
	return nil
}

func (impl *Impl) SendRecoveryRequests(nonce []byte) chan *RecoveryResponse {
	done := make(chan *RecoveryResponse)
	replicas := impl.GetOtherReplicas()
	var idx int
	for i, _ := range replicas {
		idx = i
		go func(idx int) {
			req := &RecoveryRequest{
				Nonce:     nonce,
				ReplicaId: vrImpl.Config.Id,
				DestId:    replicas[idx].Id,
			}
			log.Printf("Sending recovery request to replica %d; req: %+v", replicas[idx].Id, req)
			req.LogRequest(false)
			res := &RecoveryResponse{
				View:      -1,
				Ops:       nil,
				Nonce:     nonce,
				OpId:      -1,
				CommitId:  -1,
				ReplicaId: replicas[idx].Id,
				DestId: impl.Config.Id,
			}
			err := replicas[idx].Do("ViewManager.Recovery", req, res, false)
			if err != nil {
				return
			}
			done <- res
		}(idx)
	}
	return done
}

func (impl *Impl) WaitForRecoveryQuorum(nonce []byte, done chan *RecoveryResponse) *RecoveryResponse {
	var quorum int
	var primaryResponse *RecoveryResponse
	for quorum < impl.GetQuorumSize() {
		res := <- done
		if res != nil {
			if bytes.Equal(nonce, res.Nonce) {
				res.LogResponse(true)
				// CommitId is -2 for backups and set for primary.
				if res.CommitId != -2 {
					primaryResponse = res
				}
				quorum += 1
				// If quorum is reached but we still didn't hear from the primary, then fail the quorum and wait until primary responds.
				if quorum == impl.GetQuorumSize() && primaryResponse == nil {
					quorum -= 1
				}
			} else {
				log.Printf("[RecoveryQuorum] Received response for a different recovery request; reqNonce: %v, resNonce: %v", nonce, res.Nonce)
			}
		}
	}
	log.Printf("Recovery response from primary: %+v", primaryResponse)
	return primaryResponse
}

func (impl *Impl) IsClusterStatusNormal() bool {
	return impl.ClusterStatus == StatusNormal
}

func (impl *Impl) SetClusterStatusNormal() {
	impl.ClusterStatus = StatusNormal
	log.Printf("Cluster status changed to %s", StatusNormal)
}

func (impl *Impl) SetClusterStatusViewChange() {
	impl.ClusterStatus = StatusViewChange
	log.Printf("Cluster status changed to %s", StatusViewChange)
}

func (impl *Impl) GetClusterStatus() Status {
	return impl.ClusterStatus
}

func (impl *Impl) UpdatePrimaryNode(id int) {

	if id == impl.Config.Id {
		impl.ActivityTimeout = primaryTimeout * time.Millisecond
		impl.Config.SetPrimary(id, true)
	} else {
		impl.ActivityTimeout = backupTimeout * time.Millisecond
		impl.Config.SetPrimary(id, false)
	}

	log.Printf("Dumping configuration state; config: %+v", spew.NewFormatter(impl.Config))
}

func (impl *Impl) IsPrimaryReqRecvd() bool {
	return isPrimaryReqRecvd
}

func (impl *Impl) SetPrimaryReqRecvd(status bool) {
	isPrimaryReqRecvd = status
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
		replicas[replica.Id] = NewReplica(replica.Id, replica.Port, role)
	}
	clients := make(map[string]Client)
	for _, client := range sConfig.Clients {
		clients[client.Id] = NewClient(client.Id, client.Port)
	}

	var r = replicas[sConfig.Id]
	c := Configuration{
		Id:         sConfig.Id,
		Self:       &r,
		QuorumSize: sConfig.QuorumSize,
		Replicas:   replicas,
		clients:    clients,
		primary:    isPrimary,
	}
	log.Printf("Configuration: %+v", spew.NewFormatter(c))
	return c
}
func Init(rs *rpc.Server) {
	config := prepareConfig()
	log2.Init(strconv.Itoa(config.Id))
	var timeout time.Duration
	if config.IsPrimary() {
		timeout = primaryTimeout * time.Millisecond
	} else {
		timeout = backupTimeout * time.Millisecond
	}
	var clusterStatus = StatusNormal
	if serviceconfig.GetConfig().Recover {
		clusterStatus = StatusRecover
	}

	vrImpl = &Impl{
		Config:          config,
		ClusterStatus: clusterStatus,
		Ops: make([]log2.Operation, 0),
		OpId: -1,
		CommitId: -1,
		PrepareQueue:    make(chan *clientrpc.WorkRequest, clientrpc.MaxRequests),
		CommitQueue:     make(chan *clientrpc.WorkRequest, clientrpc.MaxRequests),
		ClientStates: make(map[string]*ClientState),
		ActivityTimer:   time.NewTimer(timeout),
		ActivityTimeout: timeout,
	}
	if vrImpl.ClusterStatus == StatusRecover {
		// Create unique nonce for the request.
		nonce := make([]byte, 12)
		if _, err := rand.Read(nonce); err != nil {
			panic(err.Error())
		}
		// Send recovery request to all Replicas.
		done := vrImpl.SendRecoveryRequests(nonce)
		// Wait for quorum to be reached and for primary to respond.
		res := vrImpl.WaitForRecoveryQuorum(nonce, done)
		// Update configuration with current state of the cluster.
		vrImpl.UpdatePrimaryNode(res.ReplicaId)
		vrImpl.View = res.View
		vrImpl.Ops = res.Ops
		for i, _ := range vrImpl.Ops {
			vrImpl.Ops[i].Committed = false
			vrImpl.Ops[i].Commit()
		}
		vrImpl.OpId = res.OpId
		vrImpl.CommitId = res.CommitId
		// Resume Normal operation of cluster.
		vrImpl.SetClusterStatusNormal()
	}

	// Only after recovery is done, will this node be available to receive requests.
	rs.RegisterName("ViewReplication", vrImpl)
	go vrImpl.ProcessPrepareRequests()
	go vrImpl.ProcessCommitRequests()
	log.Print("ViewReplication initialization successful")
}