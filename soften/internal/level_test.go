package internal

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLevelOrderOf(t *testing.T) {
	assert.Equal(t, 1, TopicLevel("L1").OrderOf())
	assert.Equal(t, 2, TopicLevel("L2").OrderOf())
	assert.Equal(t, 3, TopicLevel("L3").OrderOf())

	assert.Equal(t, 101, TopicLevel("S1").OrderOf())
	assert.Equal(t, 102, TopicLevel("S2").OrderOf())

	assert.Equal(t, -1, TopicLevel("B1").OrderOf())
	assert.Equal(t, -2, TopicLevel("B2").OrderOf())

	assert.Equal(t, -100, TopicLevel("DLQ").OrderOf())
	assert.Equal(t, -101, TopicLevel("DLQ1").OrderOf())
}

func TestLevelSuffixOf(t *testing.T) {
	assert.Equal(t, "", TopicLevel("L1").TopicSuffix())
	assert.Equal(t, "-L2", TopicLevel("L2").TopicSuffix())
	assert.Equal(t, "-L3", TopicLevel("L3").TopicSuffix())

	assert.Equal(t, "-S1", TopicLevel("S1").TopicSuffix())
	assert.Equal(t, "-S2", TopicLevel("S2").TopicSuffix())

	assert.Equal(t, "-B1", TopicLevel("B1").TopicSuffix())
	assert.Equal(t, "-B2", TopicLevel("B2").TopicSuffix())

	assert.Equal(t, "-DLQ", TopicLevel("DLQ").TopicSuffix())
	assert.Equal(t, "-DLQ1", TopicLevel("DLQ1").TopicSuffix())
}
