package log

type Operation struct {
	OpId int
	Log  LogMessage
	Result *OpResult
	Quorum []bool
}

type LogMessage struct {
	ClientId  string
	RequestId int
	Log       Message
}

type Message struct {
	Action string
	Key string
	Val *string
}

type OpResult struct {
	Val *string
	Err error
}