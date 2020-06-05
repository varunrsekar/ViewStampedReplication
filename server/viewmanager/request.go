package viewmanager

import (
	"fmt"
	"github.com/davecgh/go-spew/spew"
	"sync"
	"viewStampedReplication/clientrpc"
	log2 "viewStampedReplication/log"
)

type LockingRequests struct {
	sync.Mutex
	r map[int]clientrpc.Request
}

type StartViewRequest struct {
	View     int
	Ops      []log2.Operation
	OpId     int
	CommitId int
	ReplicaId int
	DestId int
}

func (s *StartViewRequest) LogRequest(recv bool) {
	clientrpc.Log(fmt.Sprintf("[StartViewRequest] %s start view request; req: %+v", "%s", spew.NewFormatter(s)), recv)
}

type StartViewChangeRequest struct {
	ReplicaId int
	View int
	DestId int
}

func (s *StartViewChangeRequest) LogRequest(recv bool) {
	clientrpc.Log(fmt.Sprintf("[StartViewChangeRequest] %s start view change request; req: %+v", "%s", spew.NewFormatter(s)), recv)
}

type DoViewChangeRequest struct {
	View             int
	Ops              []log2.Operation
	LatestNormalView int
	OpId             int
	CommitId         int
	ReplicaId int
	DestId int
}

func (req *DoViewChangeRequest) LogRequest(recv bool) {
	clientrpc.Log(fmt.Sprintf("[DoViewChangeRequest] %s do view change request; req: %+v", "%s", spew.NewFormatter(req)), recv)
}
