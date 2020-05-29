package viewmanager

import (
	"log"
	"sync"
	"viewStampedReplication/server/clientrpc"
	log2 "viewStampedReplication/server/log"
)

type LockingRequest struct {
	sync.Mutex
	r clientrpc.Request
}

type StartViewRequest struct {
	View     int
	Ops      []log2.Operation
	OpId     int
	CommitId int
}

func (s *StartViewRequest) LogRequest() {
	log.Printf("[StartViewRequest] Initiating request; req: %+v", s)
}

type StartViewChangeRequest struct {
	ReplicaId int
	View int
}

func (s *StartViewChangeRequest) LogRequest() {
	log.Printf("[StartViewChangeRequest] Initiating request; req: %+v", s)
}

type DoViewChangeRequest struct {
	View             int
	Ops              []log2.Operation
	LatestNormalView int
	OpId             int
	CommitId         int
}

func (req *DoViewChangeRequest) LogRequest() {
	log.Printf("[DoViewChangeRequest] Initiating DoViewChange; req: %+v", req)
}
