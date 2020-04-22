package statemachine

type State int
const (
	StateLeader State = iota
	StateFollower
	StateCandidate
	StateUnknown
)

func (s State) String() string {
	switch s {
	case StateLeader:
		return "Leader"
	case StateFollower:
		return "Follower"
	case StateCandidate:
		return "Candidate"
	default:
		return "Unknown"
	}
}
