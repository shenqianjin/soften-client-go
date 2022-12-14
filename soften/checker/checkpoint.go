package checker

// ------ consume checkpoint type ------

type ConsumeCheckpoint struct {
	CheckType CheckType
	Prev      PrevHandleCheckFunc
	Post      PostHandleCheckFunc
}

// ------ produce checkpoint type ------

type ProduceCheckpoint struct {
	CheckType CheckType
	CheckFunc PrevSendCheckFunc
}

// ------ consume checkpoints ------

func PrevHandleDiscard(checker PrevHandleCheckFunc) ConsumeCheckpoint {
	return ConsumeCheckpoint{CheckType: CheckTypePrevDiscard, Prev: checker}
}

func PostHandleDiscard(checker PostHandleCheckFunc) ConsumeCheckpoint {
	return ConsumeCheckpoint{CheckType: CheckTypePostDiscard, Post: checker}
}

func PrevHandlePending(checker PrevHandleCheckFunc) ConsumeCheckpoint {
	return ConsumeCheckpoint{CheckType: CheckTypePrevPending, Prev: checker}
}

func PostHandlePending(checker PostHandleCheckFunc) ConsumeCheckpoint {
	return ConsumeCheckpoint{CheckType: CheckTypePostPending, Post: checker}
}

func PrevHandleBlocking(checker PrevHandleCheckFunc) ConsumeCheckpoint {
	return ConsumeCheckpoint{CheckType: CheckTypePrevBlocking, Prev: checker}
}

func PostHandleBlocking(checker PostHandleCheckFunc) ConsumeCheckpoint {
	return ConsumeCheckpoint{CheckType: CheckTypePostBlocking, Post: checker}
}

func PrevHandleRetrying(checker PrevHandleCheckFunc) ConsumeCheckpoint {
	return ConsumeCheckpoint{CheckType: CheckTypePrevRetrying, Prev: checker}
}

func PostHandleRetrying(checker PostHandleCheckFunc) ConsumeCheckpoint {
	return ConsumeCheckpoint{CheckType: CheckTypePostRetrying, Post: checker}
}

func PrevHandleDead(checker PrevHandleCheckFunc) ConsumeCheckpoint {
	return ConsumeCheckpoint{CheckType: CheckTypePrevDead, Prev: checker}
}

func PostHandleDead(checker PostHandleCheckFunc) ConsumeCheckpoint {
	return ConsumeCheckpoint{CheckType: CheckTypePostDead, Post: checker}
}

func PrevHandleUpgrade(checker PrevHandleCheckFunc) ConsumeCheckpoint {
	return ConsumeCheckpoint{CheckType: CheckTypePrevUpgrade, Prev: checker}
}

func PostHandleUpgrade(checker PostHandleCheckFunc) ConsumeCheckpoint {
	return ConsumeCheckpoint{CheckType: CheckTypePostUpgrade, Post: checker}
}

func PrevHandleDegrade(checker PrevHandleCheckFunc) ConsumeCheckpoint {
	return ConsumeCheckpoint{CheckType: CheckTypePrevDegrade, Prev: checker}
}

func PostHandleDegrade(checker PostHandleCheckFunc) ConsumeCheckpoint {
	return ConsumeCheckpoint{CheckType: CheckTypePostDegrade, Post: checker}
}

func PrevHandleShift(checker PrevHandleCheckFunc) ConsumeCheckpoint {
	return ConsumeCheckpoint{CheckType: CheckTypePrevShift, Prev: checker}
}

func PostHandleShift(checker PostHandleCheckFunc) ConsumeCheckpoint {
	return ConsumeCheckpoint{CheckType: CheckTypePostShift, Post: checker}
}

func PrevHandleTransfer(checker PrevHandleCheckFunc) ConsumeCheckpoint {
	return ConsumeCheckpoint{CheckType: CheckTypePrevTransfer, Prev: checker}
}

func PostHandleTransfer(checker PostHandleCheckFunc) ConsumeCheckpoint {
	return ConsumeCheckpoint{CheckType: CheckTypePostTransfer, Post: checker}
}

// ------ produce checkpoints ------

func PrevSendDiscard(checker PrevSendCheckFunc) ProduceCheckpoint {
	return ProduceCheckpoint{CheckType: ProduceCheckTypeDiscard, CheckFunc: checker}
}

func PrevSendDead(checker PrevSendCheckFunc) ProduceCheckpoint {
	return ProduceCheckpoint{CheckType: ProduceCheckTypeDead, CheckFunc: checker}
}

func PrevSendUpgrade(checker PrevSendCheckFunc) ProduceCheckpoint {
	return ProduceCheckpoint{CheckType: ProduceCheckTypeUpgrade, CheckFunc: checker}
}

func PrevSendDegrade(checker PrevSendCheckFunc) ProduceCheckpoint {
	return ProduceCheckpoint{CheckType: ProduceCheckTypeDegrade, CheckFunc: checker}
}

func PrevSendShift(checker PrevSendCheckFunc) ProduceCheckpoint {
	return ProduceCheckpoint{CheckType: ProduceCheckTypeShift, CheckFunc: checker}
}

func PrevSendTransfer(checker PrevSendCheckFunc) ProduceCheckpoint {
	return ProduceCheckpoint{CheckType: ProduceCheckTypeTransfer, CheckFunc: checker}
}
