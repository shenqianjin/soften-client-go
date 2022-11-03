package backoff

import (
	"math/rand"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// Backoff computes the delay before retrying an action.
// It uses an exponential backoff with jitter. The jitter represents up to 20 percents of the delay.
type Backoff struct {
	backoff time.Duration
}

const (
	minBackoff       = 100 * time.Millisecond
	maxBackoff       = 60 * time.Second
	jitterPercentage = 0.2
)

// Next returns the delay to wait before next retry
func (b *Backoff) Next() time.Duration {
	// Double the delay each time
	b.backoff += b.backoff
	if b.backoff.Nanoseconds() < minBackoff.Nanoseconds() {
		b.backoff = minBackoff
	} else if b.backoff.Nanoseconds() > maxBackoff.Nanoseconds() {
		b.backoff = maxBackoff
	}
	jitter := rand.Float64() * float64(b.backoff) * jitterPercentage

	return b.backoff + time.Duration(jitter)
}
