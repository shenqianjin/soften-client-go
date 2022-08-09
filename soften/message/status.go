package message

import (
	"errors"
	"fmt"

	"github.com/shenqianjin/soften-client-go/soften/internal"
)

const (
	StatusReady    = internal.MessageStatus(internal.DefaultMessageStatusReady)
	StatusPending  = internal.MessageStatus("Pending")
	StatusBlocking = internal.MessageStatus("Blocking")
	StatusRetrying = internal.MessageStatus("Retrying")
	StatusDead     = internal.MessageStatus("Dead")
	StatusDone     = internal.MessageStatus("Done")
	StatusDiscard  = internal.MessageStatus("Discard")
	//statusNewReady = internal.MessageStatus("NewReady")
)

func StatusOf(status string) (internal.MessageStatus, error) {
	for _, v := range StatusValues() {
		if v.String() == status {
			return v, nil
		}
	}
	return "", errors.New(fmt.Sprintf("invalid message status: %s", status))
}

func StatusValues() []internal.MessageStatus {
	values := []internal.MessageStatus{
		StatusReady,
		StatusBlocking, StatusPending, StatusRetrying,
		StatusDead, StatusDone, StatusDiscard,
	}
	return values
}
