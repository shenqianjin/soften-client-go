package internal

import (
	"log"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestAvgCostPolicy(t *testing.T) {
	avg := int64(10)
	positiveJitter := 0.4
	negativeJitter := 0.2
	loop := 300
	policy := NewAvgCostPolicy(avg, positiveJitter, negativeJitter)

	costTotal := float64(0)
	min := float64(avg) * (1 - negativeJitter)
	max := float64(avg) * (1 + positiveJitter)
	for i := 0; i < loop; i++ {
		cost := float64(policy.Next() / time.Millisecond)
		assert.True(t, cost >= min)
		assert.True(t, cost < max)
		costTotal += cost
	}
	costAvg := costTotal / float64(loop)
	log.Printf("avg jitter cost policy - expect avg: %v, cost avg: %v", avg, costAvg)
	assert.True(t, math.Abs(costAvg-float64(avg)) < 1)
}
