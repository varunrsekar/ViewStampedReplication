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
}

type OpResult struct {
	Val *string
}