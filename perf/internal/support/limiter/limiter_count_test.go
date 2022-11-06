package limiter

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestConcurrencyLimiter(t *testing.T) {
	running := 0
	var runningLock sync.RWMutex
	var group sync.WaitGroup
	loop := 200
	group.Add(loop)
	limiter := NewCountLimiter(3)

	for i := 0; i < loop; i++ {
		go func(i int) {
			if !limiter.TryAcquire() {
				group.Done()
				return
			}
			runningLock.Lock()
			running++
			runningLock.Unlock()
			if i%100 == 0 {
				fmt.Println("running --- ", running)
			}
			defer func() {
				limiter.Release()
				runningLock.Lock()
				running--
				runningLock.Unlock()
			}()
			// assert
			assert.True(t, running <= 3)
			// do biz
			time.Sleep(time.Millisecond * 5)

			group.Done()

		}(i)
	}
	group.Wait()

}
