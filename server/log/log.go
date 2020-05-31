package log

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"viewStampedReplication/server/logger"
	"viewStampedReplication/server/serviceconfig"
	"viewStampedReplication/store"
)

var opLogger *logrus.Logger

func (msg Message) String() string {
	if msg.Val == nil {
		return fmt.Sprintf("%s %s", msg.Action, msg.Key)
	}
	return fmt.Sprintf("%s %s (%s)", msg.Action, msg.Key, *(msg.Val))
}

func (log LogMessage) String() string {
	return fmt.Sprintf("%s %d %s", log.ClientId, log.RequestId, log.Log)
}

func (op *Operation) Commit() *OpResult {
	msg := op.Log.Log
	opLogger.Infof("%s", op)
	var err error
	var val *string
	switch msg.Action {
	case "CREATE":
		err = store.Get().Create(msg.Key, *msg.Val)
	case "DELETE":
		err = store.Get().Delete(msg.Key)
	case "GET":
		*val, err = store.Get().Read(msg.Key)
	case "UPDATE":
		err = store.Get().Update(msg.Key, *msg.Val)
	}
	if err != nil {
		val = nil
	}
	op.Result = &OpResult{
		Val: val,
		Err: err,
	}
	return op.Result
}

func (op *Operation) IsQuorumSatisfied() bool {
	var count int
	for _, q := range op.Quorum {
		if q {
			count++
		}
	}
	if count == serviceconfig.GetConfig().QuorumSize {
		return true
	}
	return false
}

func (op Operation) String() string {
	return fmt.Sprintf("%d %s", op.OpId, op.Log)
}

func init() {
	opLogger = logger.NewLogger("op.log")
	opLogger.Info("OpId ClientId RequestId Key (Val)")
}