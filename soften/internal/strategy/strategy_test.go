package strategy

import (
	"fmt"
	"math"
	"strings"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestEachRand(t *testing.T) {
	// weight max limit
	weights := []uint{500, 10}
	strategy, err := NewEachRandStrategy(weights)
	assert.True(t, strings.Contains(err.Error(), fmt.Sprintf("weight cannot exceed %d", defaultMaxWeight)))

	// weight total invalid
	weights = []uint{0, 0}
	strategy, err = NewEachRandStrategy(weights)
	assert.True(t, strings.Contains(err.Error(), "weight total must be larger than 0"))

	// weight
	weights = []uint{100, 70, 30}
	strategy, err = NewEachRandStrategy(weights)
	assert.Nil(t, err)
	strategyHits := make(map[int]int, len(weights))
	weightTotal := uint(0)
	for _, weight := range weights {
		weightTotal += weight
	}
	weightRates := make([]float64, len(weights))
	for index, weight := range weights {
		weightRates[index] = float64(weight) / float64(weightTotal)
	}
	loop := 10 * int(weightTotal)
	for i := 0; i < loop; i++ {
		hit := strategy.Next()
		strategyHits[hit]++
	}
	for index, chosen := range strategyHits {
		assert.True(t, chosen > 0)
		expected := int(weightRates[index] * float64(loop))
		log.Infof("EachRandTest: expected: %d, chosen: %d", expected, chosen)
		assert.True(t, math.Abs(float64(expected-chosen)) < float64(loop)*0.5)
	}

	// weight exclude
	weights = []uint{100, 70, 30}
	strategy, err = NewEachRandStrategy(weights)
	assert.Nil(t, err)
	for i := 0; i < 100; i++ {
		hit := strategy.Next(1)
		assert.NotEqual(t, 1, hit)
	}
}

func TestRoundRand(t *testing.T) {
	// weight max limit
	weights := []uint{500, 10}
	strategy, err := NewRoundRandStrategy(weights)
	assert.True(t, strings.Contains(err.Error(), fmt.Sprintf("weight cannot exceed %d", defaultMaxWeight)))

	// weight total invalid
	weights = []uint{0, 0}
	strategy, err = NewRoundRandStrategy(weights)
	assert.True(t, strings.Contains(err.Error(), "weight total must be larger than 0"))

	// weight
	weights = []uint{100, 70, 30}
	strategy, err = NewRoundRandStrategy(weights)
	assert.Nil(t, err)
	strategyHits := make(map[int]int, len(weights))
	weightTotal := uint(0)
	for _, weight := range weights {
		weightTotal += weight
	}
	weightRates := make([]float64, len(weights))
	for index, weight := range weights {
		weightRates[index] = float64(weight) / float64(weightTotal)
	}
	loop := 3 * int(weightTotal)
	for i := 0; i < loop; i++ {
		hit := strategy.Next()
		strategyHits[hit]++
	}
	for index, chosen := range strategyHits {
		assert.True(t, chosen > 0)
		expected := int(weightRates[index] * float64(loop))
		log.Infof("EachRandTest: expected: %d, chosen: %d", expected, chosen)
		assert.True(t, math.Abs(float64(expected-chosen)) < float64(loop)*0.001)
	}

	// weight exclude
	weights = []uint{100, 70, 30}
	strategy, err = NewRoundRandStrategy(weights)
	assert.Nil(t, err)
	for i := 0; i < 100; i++ {
		hit := strategy.Next(1)
		assert.NotEqual(t, 1, hit)
	}
}

func TestRoundWeight(t *testing.T) {
	// weight max limit
	weights := []uint{500, 10}
	strategy, err := NewRoundWeightStrategy(weights)
	assert.True(t, strings.Contains(err.Error(), fmt.Sprintf("weight cannot exceed %d", defaultMaxWeight)))

	// weight total invalid
	weights = []uint{0, 0}
	strategy, err = NewRoundWeightStrategy(weights)
	assert.True(t, strings.Contains(err.Error(), "weight total must be larger than 0"))

	// weight
	weights = []uint{100, 70, 30}
	strategy, err = NewRoundWeightStrategy(weights)
	assert.Nil(t, err)
	strategyHits := make(map[int]int, len(weights))
	weightTotal := uint(0)
	for _, weight := range weights {
		weightTotal += weight
	}
	weightRates := make([]float64, len(weights))
	for index, weight := range weights {
		weightRates[index] = float64(weight) / float64(weightTotal)
	}
	loop := 3 * int(weightTotal)
	for i := 0; i < loop; i++ {
		hit := strategy.Next()
		strategyHits[hit]++
	}
	for index, chosen := range strategyHits {
		assert.True(t, chosen > 0)
		expected := int(weightRates[index] * float64(loop))
		log.Infof("EachRandTest: expected: %d, chosen: %d", expected, chosen)
		assert.True(t, math.Abs(float64(expected-chosen)) < float64(loop)*0.001)
	}

	// weight exclude
	weights = []uint{100, 70, 30}
	strategy, err = NewRoundWeightStrategy(weights)
	assert.Nil(t, err)
	for i := 0; i < 100; i++ {
		hit := strategy.Next(1)
		assert.NotEqual(t, 1, hit)
	}
}

func TestRoundRobin(t *testing.T) {
	// invalid round-robin length
	strategy, err := NewRoundRobinStrategy(0)
	assert.True(t, strings.Contains(err.Error(), "invalid round robin len"))

	// weight
	robinLen := 5
	strategy, err = NewRoundRobinStrategy(robinLen)
	assert.Nil(t, err)

	expextedRate := float64(1) / float64(robinLen)
	loop := 1000
	strategyHits := make(map[int]int, robinLen)
	for i := 0; i < loop; i++ {
		hit := strategy.Next()
		strategyHits[hit]++
	}
	for _, chosen := range strategyHits {
		assert.True(t, chosen > 0)
		expected := int(expextedRate * float64(loop))
		log.Infof("EachRandTest: expected: %d, chosen: %d", expected, chosen)
		assert.True(t, math.Abs(float64(expected-chosen)) < float64(loop)*0.001)
	}

	// weight exclude
	strategy, err = NewRoundRobinStrategy(robinLen)
	assert.Nil(t, err)
	for i := 0; i < 100; i++ {
		hit := strategy.Next(1)
		assert.NotEqual(t, 1, hit)
	}
}
