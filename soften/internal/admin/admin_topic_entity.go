package admin

import "time"

type TopicStats struct {
	MsgRateIn                        float64                      `json:"msgRateIn"`
	MsgThroughputIn                  float64                      `json:"msgThroughputIn"`
	MsgRateOut                       float64                      `json:"msgRateOut"`
	MsgThroughputOut                 float64                      `json:"msgThroughputOut"`
	BytesInCounter                   int                          `json:"bytesInCounter"`
	MsgInCounter                     int                          `json:"msgInCounter"`
	BytesOutCounter                  int                          `json:"bytesOutCounter"`
	MsgOutCounter                    int                          `json:"msgOutCounter"`
	AverageMsgSize                   float64                      `json:"averageMsgSize"`
	MsgChunkPublished                bool                         `json:"msgChunkPublished"`
	StorageSize                      int                          `json:"storageSize"`
	BacklogSize                      int                          `json:"backlogSize"`
	EarliestMsgPublishTimeInBacklogs int                          `json:"earliestMsgPublishTimeInBacklogs"`
	OffloadedStorageSize             int                          `json:"offloadedStorageSize"`
	Publishers                       []StatsPublisher             `json:"publishers"`
	WaitingPublishers                int                          `json:"waitingPublishers"`
	Subscriptions                    map[string]StatsSubscription `json:"subscriptions"`
	Replication                      struct {
	} `json:"replication"`
	DeduplicationStatus                              string `json:"deduplicationStatus"`
	NonContiguousDeletedMessagesRanges               int    `json:"nonContiguousDeletedMessagesRanges"`
	NonContiguousDeletedMessagesRangesSerializedSize int    `json:"nonContiguousDeletedMessagesRangesSerializedSize"`
}

type StatsPublisher struct {
	AccessMode         string  `json:"accessMode"`
	MsgRateIn          float64 `json:"msgRateIn"`
	MsgThroughputIn    float64 `json:"msgThroughputIn"`
	AverageMsgSize     float64 `json:"averageMsgSize"`
	ChunkedMessageRate float64 `json:"chunkedMessageRate"`
	ProducerId         int     `json:"producerId"`
	Metadata           struct {
	} `json:"metadata"`
	Address        string    `json:"address"`
	ConnectedSince time.Time `json:"connectedSince"`
	ClientVersion  string    `json:"clientVersion"`
	ProducerName   string    `json:"producerName"`
}

type StatsSubscription struct {
	MsgRateOut                       float64         `json:"msgRateOut"`
	MsgThroughputOut                 float64         `json:"msgThroughputOut"`
	BytesOutCounter                  int             `json:"bytesOutCounter"`
	MsgOutCounter                    int             `json:"msgOutCounter"`
	MsgRateRedeliver                 float64         `json:"msgRateRedeliver"`
	ChunkedMessageRate               int             `json:"chunkedMessageRate"`
	MsgBacklog                       int             `json:"msgBacklog"`
	BacklogSize                      int             `json:"backlogSize"`
	EarliestMsgPublishTimeInBacklog  int             `json:"earliestMsgPublishTimeInBacklog"`
	MsgBacklogNoDelayed              int             `json:"msgBacklogNoDelayed"`
	BlockedSubscriptionOnUnackedMsgs bool            `json:"blockedSubscriptionOnUnackedMsgs"`
	MsgDelayed                       int             `json:"msgDelayed"`
	UnackedMessages                  int             `json:"unackedMessages"`
	Type                             string          `json:"type"`
	ActiveConsumerName               string          `json:"activeConsumerName"`
	MsgRateExpired                   float64         `json:"msgRateExpired"`
	TotalMsgExpired                  int             `json:"totalMsgExpired"`
	LastExpireTimestamp              int             `json:"lastExpireTimestamp"`
	LastConsumedFlowTimestamp        int64           `json:"lastConsumedFlowTimestamp"`
	LastConsumedTimestamp            int64           `json:"lastConsumedTimestamp"`
	LastAckedTimestamp               int64           `json:"lastAckedTimestamp"`
	LastMarkDeleteAdvancedTimestamp  int64           `json:"lastMarkDeleteAdvancedTimestamp"`
	Consumers                        []StatsConsumer `json:"consumers"`
	AllowOutOfOrderDelivery          bool            `json:"allowOutOfOrderDelivery"`
	ConsumersAfterMarkDeletePosition struct {
	} `json:"consumersAfterMarkDeletePosition"`
	NonContiguousDeletedMessagesRanges               int  `json:"nonContiguousDeletedMessagesRanges"`
	NonContiguousDeletedMessagesRangesSerializedSize int  `json:"nonContiguousDeletedMessagesRangesSerializedSize"`
	Durable                                          bool `json:"durable"`
	Replicated                                       bool `json:"replicated"`
}

type StatsConsumer struct {
	MsgRateOut                   float64 `json:"msgRateOut"`
	MsgThroughputOut             float64 `json:"msgThroughputOut"`
	BytesOutCounter              int     `json:"bytesOutCounter"`
	MsgOutCounter                int     `json:"msgOutCounter"`
	MsgRateRedeliver             float64 `json:"msgRateRedeliver"`
	ChunkedMessageRate           float64 `json:"chunkedMessageRate"`
	ConsumerName                 string  `json:"consumerName"`
	AvailablePermits             int     `json:"availablePermits"`
	UnackedMessages              int     `json:"unackedMessages"`
	AvgMessagesPerEntry          int     `json:"avgMessagesPerEntry"`
	BlockedConsumerOnUnackedMsgs bool    `json:"blockedConsumerOnUnackedMsgs"`
	LastAckedTimestamp           int64   `json:"lastAckedTimestamp"`
	LastConsumedTimestamp        int64   `json:"lastConsumedTimestamp"`
	Metadata                     struct {
	} `json:"metadata"`
	Address        string    `json:"address"`
	ConnectedSince time.Time `json:"connectedSince"`
	ClientVersion  string    `json:"clientVersion"`
}

type TopicInternalStats struct {
	EntriesAddedCounter                int                            `json:"entriesAddedCounter"`
	NumberOfEntries                    int                            `json:"numberOfEntries"`
	TotalSize                          int                            `json:"totalSize"`
	CurrentLedgerEntries               int                            `json:"currentLedgerEntries"`
	CurrentLedgerSize                  int                            `json:"currentLedgerSize"`
	LastLedgerCreatedTimestamp         time.Time                      `json:"lastLedgerCreatedTimestamp"`
	LastLedgerCreationFailureTimestamp interface{}                    `json:"lastLedgerCreationFailureTimestamp"`
	WaitingCursorsCount                int                            `json:"waitingCursorsCount"`
	PendingAddEntriesCount             int                            `json:"pendingAddEntriesCount"`
	LastConfirmedEntry                 string                         `json:"lastConfirmedEntry"`
	State                              string                         `json:"state"`
	Ledgers                            []InternalStatsLedger          `json:"ledgers"`
	Cursors                            map[string]InternalStatsCursor `json:"cursors"`
	SchemaLedgers                      []InternalStatsSchemaLedger    `json:"schemaLedgers"`
	CompactedLedger                    InternalStatsCompactedLedger   `json:"compactedLedger"`
}

type InternalStatsLedger struct {
	LedgerId  int         `json:"ledgerId"`
	Entries   int         `json:"entries"`
	Size      int         `json:"size"`
	Offloaded bool        `json:"offloaded"`
	Metadata  interface{} `json:"metadata"`
}

type InternalStatsCursor struct {
	MarkDeletePosition                       string    `json:"markDeletePosition"`
	ReadPosition                             string    `json:"readPosition"`
	WaitingReadOp                            bool      `json:"waitingReadOp"`
	PendingReadOps                           int       `json:"pendingReadOps"`
	MessagesConsumedCounter                  int       `json:"messagesConsumedCounter"`
	CursorLedger                             int       `json:"cursorLedger"`
	CursorLedgerLastEntry                    int       `json:"cursorLedgerLastEntry"`
	IndividuallyDeletedMessages              string    `json:"individuallyDeletedMessages"`
	LastLedgerSwitchTimestamp                time.Time `json:"lastLedgerSwitchTimestamp"`
	State                                    string    `json:"state"`
	NumberOfEntriesSinceFirstNotAckedMessage int       `json:"numberOfEntriesSinceFirstNotAckedMessage"`
	TotalNonContiguousDeletedMessagesRange   int       `json:"totalNonContiguousDeletedMessagesRange"`
	Properties                               struct {
	} `json:"properties"`
}

type InternalStatsSchemaLedger struct {
	LedgerId  int         `json:"ledgerId"`
	Entries   int         `json:"entries"`
	Size      int         `json:"size"`
	Offloaded bool        `json:"offloaded"`
	Metadata  interface{} `json:"metadata"`
}

type InternalStatsCompactedLedger struct {
	LedgerId  int         `json:"ledgerId"`
	Entries   int         `json:"entries"`
	Size      int         `json:"size"`
	Offloaded bool        `json:"offloaded"`
	Metadata  interface{} `json:"metadata"`
}
