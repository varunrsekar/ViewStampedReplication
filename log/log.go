package log

import (
	"fmt"
	"github.com/davecgh/go-spew/spew"
	"github.com/sirupsen/logrus"
	"log"
	"viewStampedReplication/logger"
	"viewStampedReplication/server/serviceconfig"
	"viewStampedReplication/store"
)

var opLogger *logrus.Logger

type OpResult struct {
	Val *string
	Err error
}

type Message struct {
	Action string
	Key string
	Val *string
}

func (msg Message) String() string {
	if msg.Val == nil {
		return fmt.Sprintf("%s %s", msg.Action, msg.Key)
	}
	return fmt.Sprintf("%s %s (%s)", msg.Action, msg.Key, *(msg.Val))
}

type LogMessage struct {
	ClientId  string
	RequestId int
	Log       Message
}

func (log LogMessage) String() string {
	return fmt.Sprintf("%s %d %s", log.ClientId, log.RequestId, log.Log)
}

type Operation struct {
	OpId   int
	Log    LogMessage
	Result *OpResult
	Quorum map[int]bool
	Committed bool
}

func (op *Operation) Commit() *OpResult {
	msg := op.Log.Log
	log.Printf("Operation: %+v", spew.NewFormatter(op))
	opLogger.Infof("%+v", spew.NewFormatter(op))
	var err error
	var val string
	var resVal *string
	switch msg.Action {
	case "CREATE":
		err = store.Get().Create(msg.Key, *msg.Val)
	case "DELETE":
		err = store.Get().Delete(msg.Key)
	case "GET":
		val, err = store.Get().Read(msg.Key)
	case "UPDATE":
		err = store.Get().Update(msg.Key, *msg.Val)
	}
	if err == nil {
		resVal = &val
	}
	op.Result = &OpResult{
		Val: resVal,
		Err: err,
	}
	op.Committed = true
	return op.Result
}

func (op *Operation) IsQuorumSatisfied() bool {
	count := len(op.Quorum)
	log.Printf("Operation quorum: %d; opId: %d", count, op.OpId)
	if count == serviceconfig.GetConfig().QuorumSize {
		log.Printf("Quorum satisfied; recvd: %d", count)
		return true
	}
	return false
}

func (op Operation) String() string {
	return fmt.Sprintf("%d %s", op.OpId, op.Log)
}

func Init(id string) {
	opLogger = logger.NewLogger(fmt.Sprintf("op-%s.log", id))
	opLogger.Info("OpId ClientId RequestId Key (Val)")
}