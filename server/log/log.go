package log

import "viewStampedReplication/server/serviceconfig"

func (op *Operation) Commit() *OpResult {
	op.Result = &OpResult{}
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
