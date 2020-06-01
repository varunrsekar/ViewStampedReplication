package viewreplication

import (
	"fmt"
	"github.com/davecgh/go-spew/spew"
	"viewStampedReplication/clientrpc"
	log2 "viewStampedReplication/log"
)


type PrepareRequest struct {
	ClientId  string
	RequestId int
	View      int
	Log       log2.LogMessage
	OpId      int
	CommitId  int
	ReplicaId int
}

func (req *PrepareRequest) LogRequest(recv bool) {
	clientrpc.Log(fmt.Sprintf("[PrepareRequest] %s prepare request: %v", "%s", spew.NewFormatter(req)), recv)
}

type CommitRequest struct {
	View int
	CommitId int
	ReplicaId int
}

func (req *CommitRequest) LogRequest(recv bool) {
	clientrpc.Log(fmt.Sprintf("[CommitRequest] %s commit request: %+v", "%s", spew.NewFormatter(req)), recv)
}

type GetStateRequest struct {
	View int
	OpId int
	ReplicaId int
}

func (req *GetStateRequest) LogRequest(recv bool) {
	clientrpc.Log(fmt.Sprintf("[GetStateRequest] %s get state request: %+v", "%s", spew.NewFormatter(req)), recv)
}

type NewStateResponse struct {
	View int
	OpId int
	CommitId int
	Ops []log2.Operation
	ReplicaId int
}

func (res *NewStateResponse) LogResponse(recv bool) {
	clientrpc.Log(fmt.Sprintf("[NewStateResponse] %s new state response: %+v", "%s", spew.NewFormatter(res)), recv)
}

type OpResponse struct {
	View      int
	RequestId int
	Result    *log2.OpResult
}

func (or *OpResponse) LogResponse(recv bool) {
	clientrpc.Log(fmt.Sprintf("[OpResponse] %s operation response: %+v", "%s", spew.NewFormatter(or)), recv)
}

type RecoveryRequest struct {
	Nonce []byte
	ReplicaId int
}

func (req *RecoveryRequest) LogRequest(recv bool) {
	clientrpc.Log(fmt.Sprintf("[RecoveryRequest] %s recovery request; req: %+v", "%s", spew.NewFormatter(req)), recv)
}

type RecoveryResponse struct {
	View      int
	Ops       []log2.Operation
	Nonce     []byte
	OpId      int
	CommitId  int
	ReplicaId int
}

func (res *RecoveryResponse) LogResponse(recv bool) {
	clientrpc.Log(fmt.Sprintf("[RecoveryResponse] %s recovery response; res: %+v", "%s", spew.NewFormatter(res)), recv)
}

type PrepareOkRequest struct {
	View int
	OpId int
	ReplicaId int
}

func (req *PrepareOkRequest) LogRequest(recv bool) {
	clientrpc.Log(fmt.Sprintf("[PrepareOkRequest] %s prepare ok request: %+v", "%s", spew.NewFormatter(req)), recv)
}