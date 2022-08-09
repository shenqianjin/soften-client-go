package internal

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMessageStatusSuffixOf(t *testing.T) {
	assert.Equal(t, "", MessageStatus("Ready").TopicSuffix())
	assert.Equal(t, "-RETRYING", MessageStatus("Retrying").TopicSuffix())
	assert.Equal(t, "-PENDING", MessageStatus("Pending").TopicSuffix())
	assert.Equal(t, "-BLOCKING", MessageStatus("Blocking").TopicSuffix())
	assert.Equal(t, "-DLQ", MessageStatus("Dead").TopicSuffix())
}
