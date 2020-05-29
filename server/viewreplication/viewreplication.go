package viewreplication

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"net/rpc"
	"sync"
	"time"
	"viewStampedReplication/server/clientrpc"
	log2 "viewStampedReplication/server/log"
	"viewStampedReplication/server/serviceconfig"
)

const (
	primaryTimeout = 4800 // Timeout for primary node in milliseconds.
	backupTimeout = 5000 // Timeout for backup node in milliseconds.
)

var vrImpl *Impl

type ViewReplication interface {
	Prepare(req *PrepareRequest, res *PrepareOkResponse) error
	Commit(req *CommitRequest, res *clientrpc.EmptyResponse) error
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
	ClusterStatus            Status
	ClientStates    map[string]*ClientState
	PrepareQueue    chan *clientrpc.WorkRequest
	CommitQueue     chan *clientrpc.WorkRequest
	ActivityTimer   *time.Timer
	ActivityTimeout time.Duration
}

func (impl *Impl) Prepare(req *PrepareRequest, res *PrepareOkResponse) error {
	req.LogRequest()
	workReq := impl.BufferPrepareRequest(req)
	workReq.GetWG().Wait()
	*res = *(workReq.GetResult().(*PrepareOkResponse))
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
		impl.AdvanceOpId()
		impl.AppendOp(req.OpId, req.Log)
		impl.UpdateClientState(req.ClientId, req.RequestId, nil)
		workReq.SetResult(&PrepareOkResponse{
			View:      impl.View,
			OpId:      impl.OpId,
			ReplicaId: impl.Config.Id,
		})
		workReq.GetWG().Done()
	}
}

func (impl *Impl) Commit(req *CommitRequest, res *clientrpc.EmptyResponse) error {
	req.LogRequest()
	if req.CommitId == -1 {
		// When running for the first time without any client requests, do nothing.
		return nil
	}
	workReq := impl.BufferCommitRequest(req)
	workReq.GetWG().Wait()
	return nil
}

func (impl *Impl) BufferCommitRequest(req *CommitRequest) *clientrpc.WorkRequest {
	impl.ActivityTimer.Reset(impl.ActivityTimeout)

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
		for _, op := range impl.Ops {
			if req.CommitId == op.OpId {
				result := op.Commit()
				impl.AdvanceCommitId()
				impl.UpdateClientState(op.Log.ClientId, op.Log.RequestId, result)
			}
		}
		workReq.GetWG().Done()
	}
}

func (impl *Impl) GetState(req *GetStateRequest, res *NewStateResponse) {
	req.LogRequest()
	if req.View != impl.View {
		log.Printf("Ignoring GetStateRequest due to view mismatch; reqView: %d, currView: %d", req.View, impl.View)
		return
	}
	*res = NewStateResponse{
		View:     impl.View,
		OpId:     impl.OpId,
		CommitId: impl.CommitId,
		Ops:      make([]log2.Operation, 0),
	}
	for _, op := range impl.Ops {
		if op.OpId <= req.OpId {
			continue
		}
		res.Ops = append(res.Ops, op)
	}
	return
}

func (impl *Impl) WaitForStateTransfer(view int) {
	if view > impl.View {
		impl.OpId = impl.CommitId
		impl.ClearOpsAfterOpId()
		getStateReq := &GetStateRequest{
			View:      view,
			OpId:      impl.OpId,
			ReplicaId: impl.Config.Id,
		}
		for _, replica := range impl.GetOtherReplicas() {
			res := &NewStateResponse{
				View:     -1,
				OpId:     -1,
				CommitId: -1,
				Ops:      nil,
			}
			replica.Do("ViewManager.GetState", getStateReq, res, false)
			if res.View != view {
				// This means the replica hasn't processed the state transfer request and is ignoring it.
				continue
			}
			for _, op := range res.Ops {
				impl.Ops = append(impl.Ops, op)
			}
			impl.View = res.View
			impl.OpId = res.OpId
			impl.WaitForCommits(res.CommitId)
		}
	}
}

func (impl *Impl) WaitForCommits(commitId int) {
	for _, op := range impl.Ops {
		if op.OpId < impl.CommitId || op.OpId > commitId {
			continue
		}
		result := op.Commit()
		impl.AdvanceCommitId()
		impl.UpdateClientState(op.Log.ClientId, op.Log.RequestId, result)
	}
}

func (impl *Impl) ClearOpsAfterOpId() {
	var idx = 0
	for _, op := range impl.Ops {
		if op.OpId <= impl.OpId {
			impl.Ops[idx] = op
			idx += 1
		}
	}
	impl.Ops = impl.Ops[:idx]
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
		Quorum: make([]bool, len(impl.Config.replicas)),
	}
	impl.Ops = append(impl.Ops, op)
	return op
}

func (impl *Impl) GetBackups() []*Replica {
	backups := make([]*Replica, 0)
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
			return replica
		}
	}
	return nil
}

func (impl *Impl) GetOtherReplicas() []*Replica {
	replicas := make([]*Replica, 0)
	for _, replica := range impl.Config.replicas {
		if impl.Config.Id != replica.Id {
			replicas = append(replicas, replica)
		}
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
	if !impl.IsClusterStatusNormal() {
		log.Printf("Cluster status is not normal. Ignoring request; status: %s, req: %+v", impl.ClusterStatus, req)
		return nil
	}
	if !impl.IsPrimary() {
		log.Printf("Returning RecoveryResponse as backup; replica: %d, reqNonce: %v", impl.Config.Id, req.Nonce)
		*res = RecoveryResponse{
			View:      impl.View,
			Ops:       nil,
			Nonce:     req.Nonce,
			OpId:      -1,
			CommitId:  -1,
			ReplicaId: impl.Config.Id,
		}
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
	return nil
}

func (impl *Impl) SendRecoveryRequests(req *RecoveryRequest) chan *RecoveryResponse {
	done := make(chan *RecoveryResponse)
	for _, replica := range impl.GetOtherReplicas() {
		go func(replica *Replica) {
			res := &RecoveryResponse{
				View:      -1,
				Ops:       nil,
				Nonce:     nil,
				OpId:      -1,
				CommitId:  -1,
				ReplicaId: replica.Id,
			}
			log.Printf("[INFO] Sending Recovery request; replica: %d, req: %+v", replica.Id, req)
			replica.Do("ViewManager.Recovery", req, res, false)
			done <- res
		}(replica)
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
				res.LogResponse()
				// CommitId is -1 for backups and set for primary.
				if res.CommitId != -1 {
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
	return primaryResponse
}

func (impl *Impl) IsClusterStatusNormal() bool {
	return impl.ClusterStatus == StatusNormal
}

func (impl *Impl) SetClusterStatusNormal() {
	impl.ClusterStatus = StatusNormal
}

func (impl *Impl) SetClusterStatusViewChange() {
	impl.ClusterStatus = StatusViewChange
}

func (impl *Impl) GetClusterStatus() Status {
	return impl.ClusterStatus
}

func (impl *Impl) UpdatePrimaryNode(id int) {
	if id == impl.Config.Id {
		impl.Config.SetPrimary()
	} else {
		impl.Config.SetBackup()
	}

	for _, replica := range impl.GetOtherReplicas() {
		if id == replica.Id {
			replica.SetPrimary()
		} else {
			replica.SetBackup()
		}
	}
}

func GetImpl() *Impl {
	return vrImpl
}

func prepareConfig() Configuration {
	sConfig := serviceconfig.GetConfig()
	var role Role
	var isPrimary bool
	replicas := make(map[int]*Replica)
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
	clients := make(map[string]*Client)
	for _, client := range sConfig.Clients {
		c := clientrpc.Client{
			Hostname: fmt.Sprintf("localhost:%d", client.Port),
		}
		clients[client.Id] = NewClient(client.Id, c, client.Port)
	}

	return Configuration{
		Id:       sConfig.Id,
		Self: replicas[sConfig.Id],
		QuorumSize: sConfig.QuorumSize,
		replicas: replicas,
		clients: clients,
		primary:  isPrimary,
	}
}
func Init() {
	config := prepareConfig()
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
		vrImpl.OpId = res.OpId
		vrImpl.CommitId = res.CommitId
		// Resume Normal operation of cluster.
		vrImpl.ClusterStatus = StatusNormal
	}

	// Only after recovery is done, will this node be available to receive requests.
	rpc.RegisterName("ViewReplication", vrImpl)
	log.Print("ViewReplication initialization successful")
	go vrImpl.ProcessPrepareRequests()
	go vrImpl.ProcessCommitRequests()
}