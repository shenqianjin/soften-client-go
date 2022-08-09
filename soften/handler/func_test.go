package handler

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHandlerResult(t *testing.T) {
	assert.True(t, HandleStatusOk.handleGoto != HandleStatusAuto.handleGoto)
}
