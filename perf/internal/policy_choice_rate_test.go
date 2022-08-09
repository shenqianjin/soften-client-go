package internal

import (
	"log"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestChoiceRatePolicy(t *testing.T) {
	rate := 0.8
	loop := 500
	p := NewRateChoicePolicy(rate)

	chooseCount := 0
	for i := 0; i < loop; i++ {
		next := p.Next()
		if next == 1 {
			chooseCount++
		}
		assert.True(t, next == 0 || next == 1)
	}
	chooseRate := float64(chooseCount) / float64(loop)
	log.Printf("avg jitter cost policy - expect rate: %v, choose rate: %v", rate, chooseRate)
	assert.True(t, math.Abs(chooseRate-rate) < rate*0.1)
}
