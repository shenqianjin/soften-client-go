package message

import "github.com/shenqianjin/soften-client-go/soften/internal"

const (
	XPropertyOriginMessageID   = "X-ORIGIN-MESSAGE-ID"   // 源消息ID
	XPropertyOriginTopic       = "X-ORIGIN-TOPIC"        // 源消息Topic
	XPropertyOriginPublishTime = "X-ORIGIN-PUBLISH-TIME" // 消费发布时间
	XPropertyOriginLevel       = "X-ORIGIN-LEVEL"        // 源消息级别
	XPropertyOriginStatus      = "X-ORIGIN-STATUS"       // 源消息状态

	XPropertyPreviousMessageLevel  = "X-Previous-Level"  // 前一次消息级别
	XPropertyPreviousMessageStatus = "X-Previous-Status" // 前一次消息状态
	XPropertyPreviousErrorMessage  = "X-Previous-Error"  // 前一次消息消费报错

	XPropertyConsumeTime   = "X-Consume-Time"   // 消费时间
	XPropertyReentrantTime = "X-Reentrant-Time" // 重入时间

	XPropertyMessageCounter         = "X-Message-Counter"          // 消息计数器信息: [入队次数:消费次数:计数次数]
	XPropertyPendingMessageCounter  = "X-Pending-Message-Counter"  // Pending 消息计数器信息
	XPropertyBlockingMessageCounter = "X-Blocking-Message-Counter" // Blocking 消息计数器信息
	XPropertyRetryingMessageCounter = "X-Retrying-Message-Counter" // Retrying 消息计数器信息
	XPropertyReadyMessageCounter    = "X-Ready-Message-Counter"    // Ready 消息计数器信息
	XPropertyDeadMessageCounter     = "X-Dead-Message-Counter"     // Dead 消息计数器信息
)

var (
	statusMessageCounterMap = map[internal.MessageStatus]string{
		StatusPending:  XPropertyPendingMessageCounter,
		StatusBlocking: XPropertyBlockingMessageCounter,
		StatusRetrying: XPropertyRetryingMessageCounter,
		StatusReady:    XPropertyReadyMessageCounter,
		StatusDead:     XPropertyDeadMessageCounter,
	}
)
