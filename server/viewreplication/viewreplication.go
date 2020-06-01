package viewreplication

import (
	"bytes"
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
	return nil
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
			continue
		}
		req := workReq.GetRequest().(*PrepareRequest)
		currOpId := impl.OpId
		if req.OpId <= currOpId {
			log.Fatalf("Received invalid OpId in request; req: %v", req)
		} else if req.OpId > currOpId+1 {
			impl.WaitForStateTransfer(req.View)
		}
		if req.CommitId > impl.CommitId {
			impl.DoPendingCommits(req.CommitId)
		}
		impl.AdvanceOpId()
		impl.AppendOp(req.OpId, req.Log)
		impl.UpdateClientState(req.ClientId, req.RequestId, nil)
		okReq := &PrepareOkRequest{
			View:      impl.View,
			OpId:      req.OpId,
			ReplicaId: impl.Config.Id,
		}
		okReq.LogRequest(false)
		replica := impl.GetPrimary()
		(&replica).Do("Application.PrepareOk", okReq, &clientrpc.EmptyResponse{}, true)
		workReq.GetWG().Done()
	}
}

func (impl *Impl) Commit(req *CommitRequest, res *clientrpc.EmptyResponse) error {
	req.LogRequest(true)
	impl.ActivityTimer.Reset(impl.ActivityTimeout)
	isPrimaryReqRecvd = true
	if req.CommitId == -1 {
		// When running for the first time without any client requests, do nothing.

		return nil
	}
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
		if impl.IsPrimary() {
			continue
		}
		req := workReq.GetRequest().(*CommitRequest)
		currOpId := impl.OpId
		if req.CommitId > currOpId {
			impl.WaitForStateTransfer(req.View)
		}
		for i, _ := range impl.Ops {
			if req.CommitId == impl.Ops[i].OpId {
				if impl.Ops[i].Committed {
					log.Printf("Operation already committed: %+v", spew.NewFormatter(impl.Ops[i]))
					break
				}
				result := impl.Ops[i].Commit()
				impl.AdvanceCommitId()
				impl.UpdateClientState(impl.Ops[i].Log.ClientId, impl.Ops[i].Log.RequestId, result)
			}
		}
		workReq.GetWG().Done()
	}
}

func (impl *Impl) GetState(req *GetStateRequest) *NewStateResponse {
	req.LogRequest(true)

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
	}
	return res
}

func (impl *Impl) WaitForStateTransfer(view int) {
	if view >= impl.View {
		impl.OpId = impl.CommitId
		impl.ClearOpsAfterOpId()
		getStateReq := &GetStateRequest{
			View:      view,
			OpId:      impl.OpId,
			ReplicaId: impl.Config.Id,
		}
		getStateReq.LogRequest(false)
		replicas := impl.GetOtherReplicas()
		for i, _ := range replicas {
			var done chan bool
			var res = &NewStateResponse{
				View:      impl.View,
				OpId:      -1,
				CommitId:  -1,
				Ops:       nil,
				ReplicaId: replicas[i].Id,
			}
			go func() {
				replicas[i].Do("ViewManager.GetState", getStateReq, res, false)
				res.LogResponse(true)
				if res.View == -1 {
					// This means the replica hasn't processed the state transfer request and is ignoring it.
					done <- false
				}
				for _, op := range res.Ops {
					impl.Ops = append(impl.Ops, op)
				}
				impl.View = res.View
				impl.OpId = res.OpId
				impl.DoPendingCommits(res.CommitId)
				done <- true
			}()
			fin := <- done
			if fin {
				break
			}
		}
	}
}

func (impl *Impl) DoPendingCommits(commitId int) {
	for i, _  := range impl.Ops {
		if impl.Ops[i].OpId < impl.CommitId || impl.Ops[i].OpId > commitId {
			continue
		}
		impl.Ops[i].Committed = false
		result := impl.Ops[i].Commit()
		impl.AdvanceCommitId()
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
	if idx == 0 {
		return
	}
	impl.Ops = impl.Ops[:idx-1]
}

func (impl *Impl) AdvanceOpId() int {
	impl.OpId += 1
	return impl.OpId
}

func (impl *Impl) AdvanceCommitId() int {
	impl.CommitId += 1
	return impl.CommitId
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
	for i, replica := range impl.Config.replicas {
		if !replica.IsPrimary() {
			backups = append(backups, impl.Config.replicas[i])
		}
	}
	return backups
}

func (impl *Impl) GetPrimary() Replica {
	for i, replica := range impl.Config.replicas {
		if replica.IsPrimary() {
			return impl.Config.replicas[i]
		}
	}
	return Replica{}
}

func (impl *Impl) GetOtherReplicas() []Replica {
	replicas := make([]Replica, 0)
	for i, _ := range impl.Config.replicas {
		if impl.Config.Id != impl.Config.replicas[i].Id {
			replicas = append(replicas, impl.Config.replicas[i])
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
		LastResponse: &OpResponse{
			View:      -1,
			RequestId: -1,
			Result:    nil,
		},
	}
	return impl.ClientStates[clientId]
}

func (impl *Impl) UpdateClientState(clientId string, requestId int, res *log2.OpResult) *OpResponse {
	cs := impl.ClientStates[clientId]
	if cs == nil {
		log.Printf("Failed to lookup clientId %s; Creating new client state", clientId)
		cs = impl.CreateClientState(clientId)
	}
	cs.LastResponse = &OpResponse{
		View:      impl.View,
		RequestId: requestId,
		Result:    res,
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
	}
	res.LogResponse(false)
	return nil
}

func (impl *Impl) SendRecoveryRequests(req *RecoveryRequest) chan *RecoveryResponse {
	done := make(chan *RecoveryResponse)
	replicas := impl.GetOtherReplicas()
	var idx int
	for i, _ := range replicas {
		idx = i
		go func(idx int) {
			log.Printf("Sending recovery request to replica %d; req: %+v", replicas[idx].Id, req)
			req.LogRequest(false)
			res := &RecoveryResponse{
				View:      -1,
				Ops:       nil,
				Nonce:     nil,
				OpId:      -1,
				CommitId:  -1,
				ReplicaId: replicas[idx].Id,
			}
			replicas[idx].Do("ViewManager.Recovery", req, res, false)
			done <- res
		}(idx)
	}
	return done
}

func (impl *Impl) WaitForRecoveryQuorum(req *RecoveryRequest, done chan *RecoveryResponse) *RecoveryResponse {
	var quorum int
	var primaryResponse *RecoveryResponse
	for quorum < impl.GetQuorumSize() {
		res := <- done
		if res != nil {
			if bytes.Equal(req.Nonce, res.Nonce) {
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
				log.Printf("[RecoveryQuorum] Received response for a different recovery request; reqNonce: %v, resNonce: %v", req.Nonce, res.Nonce)
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
		Id:       sConfig.Id,
		Self: &r,
		QuorumSize: sConfig.QuorumSize,
		replicas: replicas,
		clients: clients,
		primary:  isPrimary,
	}
	log.Printf("Configuration: %+v", spew.NewFormatter(c))
	return c
}
func Init() {
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
	log.Printf("[INFO] ViewReplication Config: %+v", config)
	if vrImpl.ClusterStatus == StatusRecover {
		// If we're in recovery mode, run the Recovery protocol.
		nonce := make([]byte, 12)
		if _, err := rand.Read(nonce); err != nil {
			panic(err.Error())
		}
		req := &RecoveryRequest{
			Nonce:     nonce,
			ReplicaId: vrImpl.Config.Id,
		}
		// Send recovery request to all replicas.
		done := vrImpl.SendRecoveryRequests(req)
		// Wait for quorum to be reached and for primary to respond.
		res := vrImpl.WaitForRecoveryQuorum(req, done)
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
		vrImpl.ClusterStatus = StatusNormal
	}

	// Only after recovery is done, will this node be available to receive requests.
	rpc.RegisterName("ViewReplication", vrImpl)
	go vrImpl.ProcessPrepareRequests()
	go vrImpl.ProcessCommitRequests()
	log.Print("ViewReplication initialization successful")
}