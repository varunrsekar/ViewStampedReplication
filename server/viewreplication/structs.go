package viewreplication

import (
	"log"
	"sync"
	"viewStampedReplication/server/clientrpc"
	log2 "viewStampedReplication/server/log"
)

type Role int
const (
	RolePrimary Role = iota
	RoleBackup
	RoleUnknown
)

func (r Role) String() string {
	switch r {
	case RolePrimary:
		return "Primary"
	case RoleBackup:
		return "Replica"
	}
	return "Unknown"
}

type Replica struct {
	Id int
	c    clientrpc.Client
	role Role
	port int
}

func (r *Replica) IsPrimary() bool {
	if r.role == RolePrimary {
		return true
	}
	return false
}

type Configuration struct {
	Id int
	replicas map[int]Replica
	primary bool
	QuorumSize int
}

func (c *Configuration) IsPrimary() bool {
	return c.primary
}

type Status int
const (
	StatusNormal Status = iota
	StatusViewChange
	StatusRecover
)

func (s Status) String() string {
	switch s{
	case StatusNormal:
		return "Normal"
	case StatusViewChange:
		return "ViewChange"
	case StatusRecover:
		return "Recover"
	}
	return "Unknown"
}

type ViewReplication interface {
	Prepare(req *PrepareRequest, res *PrepareOkResponse)
	Commit(req *CommitRequest)
}

type PrepareRequest struct {
	View     int
	Log      log2.LogMessage
	OpId     int
	CommitId int
}

func (req *PrepareRequest) LogRequest() {
	log.Printf("[PrepareRequest] prepare request: %v", req)
}

type PrepareOkResponse struct {
	View int
	OpId int
	ReplicaId int
}

func (res *PrepareOkResponse) LogResponse() {
	log.Printf("[PrepareOkResponse] prepare ok response: %v", res)
}

type CommitRequest struct {
	View int
	CommitId int
}

func (req *CommitRequest) LogRequest() {
	log.Printf("[CommitRequest] commit request: %v", req)
}

type PrepareWorkRequest struct {
	req *PrepareRequest
	wg *sync.WaitGroup
	result *PrepareOkResponse
}

func (pwr *PrepareWorkRequest) GetWG() *sync.WaitGroup {
	return pwr.wg
}

func (pwr *PrepareWorkRequest) GetRequest() *PrepareRequest {
	return pwr.req
}

type Impl struct {
	Config Configuration
	OpStatus                 Status
	OpId                     int
	Ops                     []log2.Operation
	CommitId                 int
	PrepareQueue chan *clientrpc.WorkRequest
	CommitQueue chan *clientrpc.WorkRequest
}