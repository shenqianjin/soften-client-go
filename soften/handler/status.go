package handler

import (
	"github.com/shenqianjin/soften-client-go/soften/checker"
	"github.com/shenqianjin/soften-client-go/soften/internal"
)

// ------ handle status ------

type HandleStatus interface {
	GetGoto() internal.HandleGoto
	GetCheckTypes() []checker.CheckType
	GetErr() error
}

var (
	HandleStatusOk   = handleStatus{handleGoto: GotoDone} // handle message successfully
	HandleStatusAuto = handleStatus{}                     // handle message failure
)

// ------ handleStatus impl ------

type handleStatus struct {
	handleGoto internal.HandleGoto // 状态转移至新状态: 默认为空; 与当前状态一致时，参数无效。优先级比 checkTypes 高。
	checkTypes []checker.CheckType // 事后检查类型列表: 默认 DefaultPostCheckTypesInTurn; 指定时，使用指定类型列表。优先级比 handleGoto 低。
	err        error               // 后置校验器如果需要依赖处理错误，通过该参数传递。框架层在处理的过程不会更新err内容。
}

func (h handleStatus) GetGoto() internal.HandleGoto {
	return h.handleGoto
}

func (h handleStatus) GetCheckTypes() []checker.CheckType {
	return h.checkTypes
}

func (h handleStatus) GetErr() error {
	return h.err
}

func (h handleStatus) Goto(handleGoto internal.HandleGoto) handleStatus {
	h.handleGoto = handleGoto
	return h
}

func (h handleStatus) CheckTypes(types []checker.CheckType) handleStatus {
	h.checkTypes = types
	return h
}

func (h handleStatus) Err(err error) handleStatus {
	h.err = err
	return h
}

// ------ handle status builder ------

type handleStatusBuilder struct {
	handleGoto internal.HandleGoto
	checkTypes []checker.CheckType
	err        error
}

func HandleStatusBuilder() *handleStatusBuilder {
	return &handleStatusBuilder{}
}

func (b *handleStatusBuilder) Goto(handleGoto internal.HandleGoto) *handleStatusBuilder {
	b.handleGoto = handleGoto
	return b
}

func (b *handleStatusBuilder) CheckTypes(checkTypes []checker.CheckType) *handleStatusBuilder {
	b.checkTypes = checkTypes
	return b
}

func (b *handleStatusBuilder) Err(err error) *handleStatusBuilder {
	b.err = err
	return b
}

func (b *handleStatusBuilder) Build() handleStatus {
	return handleStatus{
		handleGoto: b.handleGoto,
		checkTypes: b.checkTypes,
		err:        b.err,
	}
}
