package internal

import (
	"math/rand"
	"time"
)

type ChoicePolicy interface {
	Next() uint
}

// ------ init helper ------

func init() {
	rand.Seed(time.Now().UnixNano())
}
