package log

type Operation struct {
	OpId int
	Log  LogMessage
}

type LogMessage struct {
	ClientId  string
	RequestId int
	Log       Message
}

type Message struct {
	Action string

}


