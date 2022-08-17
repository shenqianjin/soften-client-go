package internal

// ------ status enables ------

type StatusEnables struct {
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
