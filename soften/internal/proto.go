package internal

import "time"

var DebugMode = false

var RFC3339TimeInSecondPattern = "20060102150405.999"

// EarliestEventTime 暂设定只有EventTime在20年内的时间才算有效时间。
// Pulsar Go Client 零值 EventTime 非0 (https://github.com/apache/pulsar-client-go/pull/843),
var EarliestEventTime = time.Now().Add(-20 * 12 * 30 * 24 * time.Hour)
