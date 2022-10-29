
## 业务系统使用Pulsar消息中间件注意点

### 部署配置

1. 内存设置
- JVM最小堆最大堆设置相同
- 直接内存设置为最大堆的2倍
- 例如:
```text
PULSAR_MEM=${PULSAR_MEM:-"-Xms4g -Xmx4g -XX:MaxDirectMemorySize=4g"}
```
- ref: https://github.com/apache/pulsar/issues/7574

2. redelivery count 对 Exclusive 和 Failover 订阅类型 无效。使用Nack时当心。
- ref: https://github.com/apache/pulsar/issues/15836

3. Pulsar自动创建的topic类型为non-partitioned topic, non-partitioned转partitioned topic目前只能通过删除的方式。
- ref: https://github.com/apache/pulsar/issues/9739

4. Go client 暂不支持 Batch index级别的Ack/Nack (Java已支持)。当前对Batch消息的处理若中途失败,会重复消费。
- ref: https://github.com/apache/pulsar-client-go/issues/821

### 配置建议

| 配置项                                    | 默认值                          | 建议调整  | 备注                  |
|:---------------------------------------|:-----------------------------|:------|:--------------------|
| systemTopicEnabled                     | false                        | true  | ?                   |
| topicLevelPoliciesEnabled              | false                        | true  | 支持topic策略           |
| managedLedgerDefaultEnsembleSize       | 2                            | 3     | ?                   |
| managedLedgerDefaultWriteQuorum        | 2                            | 3     | ?                   |
| managedLedgerDefaultWriteQuorum        | 2                            | 3     | ?                   |
| brokerDeleteInactiveTopicsEnabled      | true                         | false | TODO: 不要自动清理没有订阅的数据 |
| brokerDeleteInactiveTopicsMode         | delete_when_no_subscriptions | -     | -                   |
| allowAutoTopicCreation                 | true                         | false | TODO: 防止Topic泛滥     |
| allowAutoTopicCreationType             | non-partitioned              | -     | -                   |
| acknowledgmentAtBatchIndexLevelEnabled | false                        | true  | Go Client暂无意义       |

建议关闭 allowAutoTopicCreationType
- 防止 Topic 泛滥
- 普通 Topic 和分区 Topic 无法互相转换，可以通过创建单分区的分区 Topic，方便后续扩展
- 通过管理流 API 手动创建