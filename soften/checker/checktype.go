package checker

// ------ check type definition ------

type CheckType string

func (e *CheckType) String() string {
	return string(*e)
}

// ------ produce check type enums ------

const (
	ProduceCheckTypeDiscard  = CheckType("Discard")
	ProduceCheckTypePending  = CheckType("Pending")
	ProduceCheckTypeBlocking = CheckType("Blocking")
	ProduceCheckTypeRetrying = CheckType("Retrying")
	ProduceCheckTypeDead     = CheckType("Dead")
	ProduceCheckTypeUpgrade  = CheckType("Upgrade")
	ProduceCheckTypeDegrade  = CheckType("Degrade")
	ProduceCheckTypeShift    = CheckType("Shift")
	ProduceCheckTypeTransfer = CheckType("Transfer")
)

func DefaultPrevSendCheckOrders() []CheckType {
	values := []CheckType{ProduceCheckTypeDiscard, ProduceCheckTypeDead,
		ProduceCheckTypeTransfer,
		ProduceCheckTypeUpgrade, ProduceCheckTypeDegrade, ProduceCheckTypeShift,
		ProduceCheckTypeBlocking, ProduceCheckTypePending, ProduceCheckTypeRetrying}
	return values
}

// ------ consume check type enums ------

const (
	CheckTypePrevDiscard  = CheckType("PrevDiscard")
	CheckTypePrevDead     = CheckType("PrevDead")
	CheckTypePrevPending  = CheckType("PrevPending")
	CheckTypePrevBlocking = CheckType("PrevBlocking")
	CheckTypePrevRetrying = CheckType("PrevRetrying")
	CheckTypePrevUpgrade  = CheckType("PrevUpgrade")
	CheckTypePrevDegrade  = CheckType("PrevDegrade")
	CheckTypePrevShift    = CheckType("PrevShift")
	CheckTypePrevTransfer = CheckType("PrevTransfer")

	CheckTypePostDiscard  = CheckType("PostHandleDiscard")
	CheckTypePostDead     = CheckType("PostHandleDead")
	CheckTypePostPending  = CheckType("PostHandlePending")
	CheckTypePostBlocking = CheckType("PostHandleBlocking")
	CheckTypePostRetrying = CheckType("PostHandleRetrying")
	CheckTypePostUpgrade  = CheckType("PostHandleUpgrade")
	CheckTypePostDegrade  = CheckType("PostHandleDegrade")
	CheckTypePostShift    = CheckType("PostHandleShift")
	CheckTypePostTransfer = CheckType("PostHandleTransfer")
)

func DefaultPrevHandleCheckOrders() []CheckType {
	values := []CheckType{CheckTypePrevDiscard, CheckTypePrevDead,
		CheckTypePrevTransfer,
		CheckTypePrevUpgrade, CheckTypePrevDegrade, CheckTypePrevShift,
		CheckTypePrevBlocking, CheckTypePrevPending, CheckTypePrevRetrying}
	return values
}

func DefaultPostHandleCheckOrders() []CheckType {
	values := []CheckType{CheckTypePostDiscard, CheckTypePostDead,
		CheckTypePostTransfer,
		CheckTypePostUpgrade, CheckTypePostDegrade, CheckTypePostShift,
		CheckTypePostBlocking, CheckTypePostPending, CheckTypePostRetrying}
	return values
}
