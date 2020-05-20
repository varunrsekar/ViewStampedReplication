package viewmanager

import (
	"log"
	"sync"
	"viewStampedReplication/server/clientrpc"
	log2 "viewStampedReplication/server/log"
	"viewStampedReplication/server/viewreplication"
)

type LockingRequest struct {
	sync.Mutex
	r clientrpc.Request
}

type Impl struct {
	vrImpl                   *viewreplication.Impl
	ReplicaId                int
	View                     int
	pendingViewChangeRequest LockingRequest
}

type ViewManager interface {
	StartView(req *StartViewRequest) error
	StartViewChange(req *StartViewChangeRequest) error
	DoViewChange(req *DoViewChangeRequest) error
}

type StartViewRequest struct {
	View     int
	Log      log2.LogMessage
	OpId     int
	CommitId int
}

func (s *StartViewRequest) LogRequest() {
	log.Printf("[StartViewRequest] Initiating request; req: %v", s)
}

type StartViewChangeRequest struct {
	ReplicaId int
	View int
}

func (s *StartViewChangeRequest) LogRequest() {
	log.Printf("[StartViewChangeRequest] Initiating request; req: %v", s)
}

type DoViewChangeRequest struct {
	View             int
	Log              log2.LogMessage
	LatestNormalView int
	OpId             int
	CommitId         int
}

func (s *DoViewChangeRequest) LogRequest() {
	log.Printf("[DoViewChangeRequest] Initiating request; req: %v", s)
}
