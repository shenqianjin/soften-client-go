package choice

import (
	"log"
	"math"
	"testing"

	"github.com/shenqianjin/soften-client-go/soften/handler"
	"github.com/stretchr/testify/assert"
)

func TestRoundRandWeightGotoPolicy(t *testing.T) {
	weightMap := make(map[string]uint64)
	weightMap[handler.StatusDone.GetGoto().String()] = 19
	weightMap[handler.StatusRetrying.GetGoto().String()] = 5
	weightMap[handler.StatusPending.GetGoto().String()] = 5
	weightMap[handler.StatusBlocking.GetGoto().String()] = 5
	weightMap[handler.StatusDiscard.GetGoto().String()] = 1

	chooseMap := make(map[string]int)
	loop := 500
	policy := NewRoundRandWeightGotoPolicy(weightMap)
	for i := 0; i < loop; i++ {
		next := policy.Next()
		chooseMap[next.(string)]++
		assert.True(t, next != nil)
	}
	var totalWeight uint64
	for _, weight := range weightMap {
		totalWeight = totalWeight + weight
	}
	for status, weight := range weightMap {
		expectedRate := float64(weight) / float64(totalWeight)
		chooseRate := float64(chooseMap[status]) / float64(loop)
		log.Printf("weighted round rand goto policy - expect rate: %v, chosen rate: %v", expectedRate, chooseRate)
		assert.True(t, math.Abs(expectedRate-chooseRate) < expectedRate*0.1)
	}

}
