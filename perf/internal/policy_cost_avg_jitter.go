package internal

import (
	"math/rand"
	"time"
)

type CostPolicy interface {
	Next() time.Duration
}

type avgCostPolicy struct {
	avg            float64
	positiveJitter float64
	negativeJitter float64

	base        float64
	jitterRange float64
}

func NewAvgCostPolicy(avg int64, positiveJitter, negativeJitter float64) *avgCostPolicy {
	if avg < 0 {
		avg = 0
	}
	if positiveJitter < 0 {
		positiveJitter = 0
	}
	if negativeJitter < 0 {
		negativeJitter = 0
	}
	policy := &avgCostPolicy{
		avg:            float64(avg),
		positiveJitter: positiveJitter,
		negativeJitter: negativeJitter,
	}
	policy.base = policy.avg * (1 - policy.negativeJitter)
	policy.jitterRange = policy.positiveJitter + policy.negativeJitter
	return policy
}

func (p *avgCostPolicy) Next() time.Duration {
	cost := p.base + rand.Float64()*p.jitterRange*p.avg
	if cost < 0 {
		return 0
	}
	return time.Duration(cost) * time.Millisecond
}
