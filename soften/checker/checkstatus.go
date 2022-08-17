package checker

import "github.com/shenqianjin/soften-client-go/soften/decider"

// ------ check status implementation ------

type checkStatus struct {
	passed       bool
	handledDefer func()
	gotoExtra    decider.GotoExtra
}

func (s *checkStatus) WithPassed(passed bool) *checkStatus {
	r := &checkStatus{
		passed:       passed,
		handledDefer: s.handledDefer,
		gotoExtra:    s.gotoExtra,
	}
	return r
}

func (s *checkStatus) WithHandledDefer(handledDefer func()) *checkStatus {
	r := &checkStatus{
		passed:       s.passed,
		handledDefer: handledDefer,
		gotoExtra:    s.gotoExtra,
	}
	return r
}

func (s *checkStatus) WithGotoExtra(gotoExtra decider.GotoExtra) *checkStatus {
	r := &checkStatus{
		passed:       s.passed,
		handledDefer: s.handledDefer,
		gotoExtra:    gotoExtra,
	}
	return r
}

func (s *checkStatus) IsPassed() bool {
	return s.passed
}

func (s *checkStatus) GetHandledDefer() func() {
	return s.handledDefer
}

func (s *checkStatus) GetGotoExtra() decider.GotoExtra {
	return s.gotoExtra
}

// ------ check status enums ------

var (
	CheckStatusPassed   = &checkStatus{passed: true}
	CheckStatusRejected = &checkStatus{passed: false}
)
