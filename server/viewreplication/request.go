package viewreplication

import (
	"log"
	"sync"
	log2 "viewStampedReplication/server/log"
)


type PrepareRequest struct {
	ClientId string
	RequestId int
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

type GetStateRequest struct {
	View int
	OpId int
	ReplicaId int
}

func (req *GetStateRequest) LogRequest() {
	log.Printf("[GetStateRequest] get state request: %+v", req)
}

type NewStateResponse struct {
	View int
	OpId int
	CommitId int
	Ops []log2.Operation
}

func (res *NewStateResponse) LogResponse() {
	log.Printf("[NewStateResponse] new state response: %+v", res)
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

type OpResponse struct {
	View      int
	RequestId int
	Result    *log2.OpResult
}

func (or *OpResponse) LogResponse() {
	log.Printf("[OpResponse] operation response: %+v", or)
}

type RecoveryRequest struct {
	Nonce []byte
	ReplicaId int
}

func (req *RecoveryRequest) LogRequest() {
	log.Printf("[RecoveryRequest] Recovery request; req: %+v", req)
}

type RecoveryResponse struct {
	View      int
	Ops       []log2.Operation
	Nonce     []byte
	OpId      int
	CommitId  int
	ReplicaId int
}

func (res *RecoveryResponse) LogResponse() {
	log.Printf("[RecoveryResponse] Recovery response; res: %+v", res)
}

type PrepareOkRequest struct {
	View int
	OpId int
	ReplicaId int
}

func (req *PrepareOkRequest) LogRequest() {
	log.Printf("[PrepareOkResponst] prepare ok request: %v", req)
}