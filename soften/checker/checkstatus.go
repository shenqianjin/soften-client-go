package checker

// ------ check status implementation ------

type checkStatus struct {
	passed       bool
	handledDefer func()
	//CheckTo      MessageGoto
	rerouteTopic string
}

func (s *checkStatus) WithPassed(passed bool) *checkStatus {
	s.passed = passed
	return s
}

func (s *checkStatus) WithHandledDefer(handledDefer func()) *checkStatus {
	r := &checkStatus{passed: s.passed, handledDefer: s.handledDefer, rerouteTopic: s.rerouteTopic}
	r.handledDefer = handledDefer
	return r
}

func (s *checkStatus) WithRerouteTopic(topic string) *checkStatus {
	r := &checkStatus{passed: s.passed, handledDefer: s.handledDefer, rerouteTopic: s.rerouteTopic}
	r.rerouteTopic = topic
	return r
}

func (s *checkStatus) IsPassed() bool {
	return s.passed
}

func (s *checkStatus) GetHandledDefer() func() {
	return s.handledDefer
}

func (s *checkStatus) GetRerouteTopic() string {
	return s.rerouteTopic
}

// ------ check status enums ------

var (
	CheckStatusPassed   = &checkStatus{passed: true}
	CheckStatusRejected = &checkStatus{passed: false}
)
