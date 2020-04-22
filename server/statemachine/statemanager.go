package statemachine

import "sync"

type StateManager struct {
	*sync.RWMutex
	currState State
	prevState State
}

func (sm *StateManager) SetState(s State) {
	sm.Lock()
	defer sm.Unlock()
	sm.prevState = sm.currState
	sm.currState = s
}

func (sm *StateManager) GetState() State {
	sm.RLock()
	defer sm.RUnlock()
	return sm.currState
}
