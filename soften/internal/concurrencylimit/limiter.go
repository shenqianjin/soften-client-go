package concurrencylimit

import (
	"fmt"
)

type Limiter interface {
	TryAcquire() bool
	Acquire()
	Release()
}

type limiter struct {
	ch chan struct{}
}

func New(n int) Limiter {
	if n <= 0 {
		panic(fmt.Sprintf("concurrency should more than one. current: %d", n))
	}
	ch := make(chan struct{}, n)
	for i := 0; i < n; i++ {
		ch <- struct{}{}
	}
	return &limiter{ch: ch}
}

func (l *limiter) TryAcquire() bool {
	select {
	case <-l.ch:
		return true
	default:
		return false
	}
}

func (l *limiter) Acquire() {
	<-l.ch
}

func (l *limiter) Release() {
	l.ch <- struct{}{}
}
