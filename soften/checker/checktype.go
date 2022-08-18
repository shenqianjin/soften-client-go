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
	ProduceCheckTypeRoute    = CheckType("Route")
)

func DefaultPrevSendCheckOrders() []CheckType {
	values := []CheckType{ProduceCheckTypeDiscard, ProduceCheckTypeDead,
		ProduceCheckTypeRoute, ProduceCheckTypeUpgrade, ProduceCheckTypeDegrade,
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
	CheckTypePrevReroute  = CheckType("PrevReroute")

	CheckTypePostDiscard  = CheckType("PostHandleDiscard")
	CheckTypePostDead     = CheckType("PostHandleDead")
	CheckTypePostPending  = CheckType("PostHandlePending")
	CheckTypePostBlocking = CheckType("PostHandleBlocking")
	CheckTypePostRetrying = CheckType("PostHandleRetrying")
	CheckTypePostUpgrade  = CheckType("PostHandleUpgrade")
	CheckTypePostDegrade  = CheckType("PostHandleDegrade")
	CheckTypePostReroute  = CheckType("PostHandleReroute")
)

func DefaultPrevHandleCheckOrders() []CheckType {
	values := []CheckType{CheckTypePrevDiscard, CheckTypePrevDead,
		CheckTypePrevReroute, CheckTypePrevUpgrade, CheckTypePrevDegrade,
		CheckTypePrevBlocking, CheckTypePrevPending, CheckTypePrevRetrying}
	return values
}

func DefaultPostHandleCheckOrders() []CheckType {
	values := []CheckType{CheckTypePostDiscard, CheckTypePostDead,
		CheckTypePostReroute, CheckTypePostUpgrade, CheckTypePostDegrade,
		CheckTypePostBlocking, CheckTypePostPending, CheckTypePostRetrying}
	return values
}
