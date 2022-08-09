package internal

import (
	"log"
	"math"
	"testing"

	"github.com/shenqianjin/soften-client-go/soften/handler"

	"github.com/stretchr/testify/assert"
)

func TestRoundRandWeightGotoPolicy(t *testing.T) {
	weightMap := make(map[string]uint)
	weightMap[string(handler.GotoDone)] = 19
	weightMap[string(handler.GotoRetrying)] = 5
	weightMap[string(handler.GotoPending)] = 5
	weightMap[string(handler.GotoBlocking)] = 5
	weightMap[string(handler.GotoDiscard)] = 1

	chooseMap := make(map[string]int)
	loop := 500
	policy := NewRoundRandWeightGotoPolicy(weightMap)
	for i := 0; i < loop; i++ {
		next := policy.Next()
		chooseMap[next.(string)]++
		assert.True(t, next != nil)
	}
	for status, weight := range weightMap {
		expectedRate := float64(weight) / float64(policy.total)
		chooseRate := float64(chooseMap[status]) / float64(loop)
		log.Printf("weighted round rand goto policy - expect rate: %v, chosen rate: %v", expectedRate, chooseRate)
		assert.True(t, math.Abs(expectedRate-chooseRate) < expectedRate*0.1)
	}

}
