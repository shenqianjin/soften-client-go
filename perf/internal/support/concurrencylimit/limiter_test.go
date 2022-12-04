package concurrencylimit

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestConcurrencyLimit(t *testing.T) {
	concurrency := 3
	loop := 200
	running := int64(0)
	var group sync.WaitGroup
	group.Add(loop)
	go func() {
		for {
			time.Sleep(200 * time.Millisecond)
			fmt.Println("running --- ", atomic.LoadInt64(&running))
		}
	}()

	limiter := New(concurrency)
	for i := 0; i < loop; i++ {
		go func(i int) {
			defer group.Done()
			limiter.Acquire()
			defer limiter.Release()
			atomic.AddInt64(&running, 1)
			defer atomic.AddInt64(&running, -1)
			// assert
			assert.True(t, atomic.LoadInt64(&running) <= int64(concurrency))
			// do biz
			time.Sleep(time.Millisecond * 10)
		}(i)
	}
	group.Wait()

}

func TestConcurrencyLimitByTryRequire(t *testing.T) {
	concurrency := 3
	loop := 200
	running := int64(0)
	var group sync.WaitGroup
	group.Add(loop)
	go func() {
		for {
			time.Sleep(2 * time.Millisecond)
			fmt.Println("running --- ", atomic.LoadInt64(&running))
		}
	}()

	limiter := New(concurrency)
	for i := 0; i < loop; i++ {
		go func(i int) {
			defer group.Done()
			if !limiter.TryAcquire() {
				return
			}
			defer limiter.Release()
			atomic.AddInt64(&running, 1)
			defer atomic.AddInt64(&running, -1)
			// assert
			assert.True(t, atomic.LoadInt64(&running) <= int64(concurrency))
			// do biz
			time.Sleep(time.Millisecond * 10)
		}(i)
	}
	group.Wait()
}
