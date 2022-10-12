package internal

// ------ status enables ------

type ProduceEnables struct {
	DiscardEnable  bool
	DeadEnable     bool
	UpgradeEnable  bool
	DegradeEnable  bool
	ShiftEnable    bool
	TransferEnable bool
}

// ------ status enables ------

type ConsumeEnables struct {
	ReadyEnable    bool
	BlockingEnable bool
	PendingEnable  bool
	RetryingEnable bool
	UpgradeEnable  bool
	DegradeEnable  bool
	ShiftEnable    bool
	DeadEnable     bool
	DiscardEnable  bool
	TransferEnable bool
}
