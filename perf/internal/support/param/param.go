package param

type GotoParam struct {
	Weight string // 处理决策权重, 固定顺序: [done, discard, dead, pending, blocking, retrying, upgrade, degrade]

	DoneWeight     uint64
	DiscardWeight  uint64
	DeadWeight     uint64
	PendingWeight  uint64
	BlockingWeight uint64
	RetryingWeight uint64
	UpgradeWeight  uint64
	DegradeWeight  uint64
	ShiftWeight    uint64
	TransferWeight uint64
}
