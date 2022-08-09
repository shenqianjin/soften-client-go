package internal

import (
	"math/rand"
)

type rateChoicePolicy struct {
	rate float64
}

func NewRateChoicePolicy(rate float64) *rateChoicePolicy {
	if rate < 0 {
		rate = 0
	}
	policy := &rateChoicePolicy{
		rate: rate,
	}

	return policy
}

func (p *rateChoicePolicy) Next() uint {
	if p.rate <= 0 {
		return 0
	}
	r := rand.Float64()
	if r < p.rate {
		return 1
	} else {
		return 0
	}
}
