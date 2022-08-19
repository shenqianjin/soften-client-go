package message

import "github.com/shenqianjin/soften-client-go/soften/internal"

const (
	XPropertyOriginMessageID   = "X-ORIGIN-MESSAGE-ID"   // 源消息ID
	XPropertyOriginTopic       = "X-ORIGIN-TOPIC"        // 源消息Topic
	XPropertyOriginPublishTime = "X-ORIGIN-PUBLISH-TIME" // 消费发布时间
	XPropertyOriginLevel       = "X-ORIGIN-LEVEL"        // 源消息级别

	XPropertyPreviousMessageStatus         = "X-Previous-Status"                  // 前一次消息状态
	XPropertyCurrentMessageStatus          = "X-Current-Status"                   // 当前消息的状态
	XPropertyReconsumeTimes                = "X-Reconsume-Times"                  // 总重试消费次数
	XPropertyReconsumeTime                 = "X-Reconsume-Time"                   // 消费时间
	XPropertyReentrantTime                 = "X-Reentrant-Time"                   // 重入时间
	XPropertyReentrantStartRedeliveryCount = "X-Reentrant-Start-Redelivery-Count" // 当前状态开始的消费次数

	XPropertyRerouteFrom  = "X-Reroute-From"  // 重路由源Topic
	XPropertyRerouteTime  = "X-Reroute-Time"  // 重路由时间
	XPropertyRerouteTimes = "X-Reroute-Times" // 重路由次数

	XPropertyUpgradeFrom  = "X-Upgrade-From"  // 升级源Topic
	XPropertyUpgradeTime  = "X-Upgrade-Time"  // 升级时间
	XPropertyUpgradeTimes = "X-Reroute-Times" // 升级次数

	XPropertyDegradeFrom  = "X-Degrade-From"  // 降级源Topic
	XPropertyDegradeTime  = "X-Degrade-Time"  // 降级时间
	XPropertyDegradeTimes = "X-Degrade-Times" // 降级次数

	XPropertyPendingReconsumeTimes  = "X-Pending-Reconsume-Times"  // Pending 状态消费次数
	XPropertyPendingReentrantTimes  = "X-Pending-Reentrant-Times"  // Pending 状态重入次数
	XPropertyBlockingReconsumeTimes = "X-Blocking-Reconsume-Times" // Blocking 状态消费次数
	XPropertyBlockingReentrantTimes = "X-Blocking-Reentrant-Times" // Blocking 状态重入次数
	XPropertyRetryingReconsumeTimes = "X-Retrying-Reconsume-Times" // Retrying 状态消费次数
	XPropertyRetryingReentrantTimes = "X-Retrying-Reentrant-Times" // Retrying 状态重入次数
	XPropertyReadyReconsumeTimes    = "X-Ready-Reconsume-Times"    // Ready 状态消费次数
	XPropertyReadyReentrantTimes    = "X-Ready-Reentrant-Times"    // Ready 状态重入次数
	XPropertyDeadReconsumeTimes     = "X-Dead-Reconsume-Times"     // Dead 状态消费次数
	XPropertyDeadReentrantTimes     = "X-Dead-Reentrant-Times"     // Dead 状态重入次数
)

func XPropertyConsumeTimes(status internal.MessageStatus) (string, bool) {
	prop, ok := statusConsumeTimesMap[status]
	return prop, ok
}

func XPropertyReentrantTimes(status internal.MessageStatus) (string, bool) {
	prop, ok := statusReentrantTimesMap[status]
	return prop, ok
}

var (
	statusConsumeTimesMap = map[internal.MessageStatus]string{
		StatusPending:  XPropertyPendingReconsumeTimes,
		StatusBlocking: XPropertyBlockingReconsumeTimes,
		StatusRetrying: XPropertyRetryingReconsumeTimes,
		StatusReady:    XPropertyReadyReconsumeTimes,
		StatusDead:     XPropertyDeadReconsumeTimes,
	}

	statusReentrantTimesMap = map[internal.MessageStatus]string{
		StatusPending:  XPropertyPendingReentrantTimes,
		StatusBlocking: XPropertyBlockingReentrantTimes,
		StatusRetrying: XPropertyRetryingReentrantTimes,
		StatusReady:    XPropertyReadyReentrantTimes,
		StatusDead:     XPropertyDeadReentrantTimes,
	}
)
