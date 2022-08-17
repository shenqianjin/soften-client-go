package checker

import (
	"context"

	"github.com/shenqianjin/soften-client-go/soften/decider"
	"github.com/shenqianjin/soften-client-go/soften/message"
)

// ------ handle check func (for consumer handle message) ------

type PrevHandleCheckFunc func(ctx context.Context, msg message.Message) CheckStatus

type PostHandleCheckFunc func(ctx context.Context, msg message.Message, err error) CheckStatus

// ------ send check func (for producer send message) ------

type PrevSendCheckFunc func(ctx context.Context, msg *message.ProducerMessage) CheckStatus

// ------ check status interface ------

type CheckStatus interface {
	IsPassed() bool
	GetHandledDefer() func()

	// GetGotoExtra extra data for specified goto status, includes Transfer, transfer currently.
	GetGotoExtra() decider.GotoExtra
}
