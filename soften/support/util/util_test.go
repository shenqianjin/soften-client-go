package util

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTopicName(t *testing.T) {
	parsedTopic, err := ParseTopicName("Test")
	assert.Nil(t, err)
	assert.Equal(t, "persistent://public/default/Test", parsedTopic)

	parsedTopic, err = ParseTopicName("default2/Test")
	assert.NotNil(t, err)
	assert.True(t, strings.Contains(err.Error(), "Invalid short topic name"))

	parsedTopic, err = ParseTopicName("public2/default2/Test")
	assert.Nil(t, err)
	assert.Equal(t, "persistent://public2/default2/Test", parsedTopic)

	parsedTopic, err = ParseTopicName("persistent://public2/default2/Test")
	assert.Nil(t, err)
	assert.Equal(t, "persistent://public2/default2/Test", parsedTopic)

	parsedTopic, err = ParseTopicName("persistent3://public2/default2/Test")
	assert.NotNil(t, err)
	assert.True(t, strings.Contains(err.Error(), "Invalid topic domain"))

	parsedTopic, err = ParseTopicName("non-persistent://public2/default2/Test")
	assert.Nil(t, err)
	assert.Equal(t, "non-persistent://public2/default2/Test", parsedTopic)
}
