package checker

// ------ check status implementation ------

type checkStatus struct {
	passed       bool
	handledDefer func()

	// extra for route/reroute

	routeTopic string
}

func (s *checkStatus) WithPassed(passed bool) *checkStatus {
	r := &checkStatus{passed: passed, handledDefer: s.handledDefer, routeTopic: s.routeTopic}
	return r
}

func (s *checkStatus) WithHandledDefer(handledDefer func()) *checkStatus {
	r := &checkStatus{passed: s.passed, handledDefer: handledDefer, routeTopic: s.routeTopic}
	return r
}

func (s *checkStatus) WithRerouteTopic(topic string) *checkStatus {
	r := &checkStatus{passed: s.passed, handledDefer: s.handledDefer, routeTopic: topic}
	return r
}

func (s *checkStatus) IsPassed() bool {
	return s.passed
}

func (s *checkStatus) GetHandledDefer() func() {
	return s.handledDefer
}

func (s *checkStatus) GetRouteTopic() string {
	return s.routeTopic
}

// ------ check status enums ------

var (
	CheckStatusPassed   = &checkStatus{passed: true}
	CheckStatusRejected = &checkStatus{passed: false}
)
