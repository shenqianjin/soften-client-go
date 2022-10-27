
后续增强：
- 利用Nack做时间粒度更小的延迟
    ```go
    replace github.com/apache/pulsar-client-go v0.9.0 => github.com/shenqianjin/pulsar-client-go v0.8.1-0.20221018115513-477c3bc320f8
    ```
    - 当前 Pulsar GO SDK中，Nack 对batch index level不支持
    - Exclusive 和 Failover Nack时 获取不到 redelivery次数。
- 