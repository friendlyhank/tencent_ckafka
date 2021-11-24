package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"strings"
	config2 "tencent_ckafka/config"
)

func main() {

	cfg, err := config2.ParseConfig("./config/config.json")
	if err != nil {
		log.Fatal(err)
	}

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		// 设置接入点，请通过控制台获取对应Topic的接入点。
		"bootstrap.servers": strings.Join(cfg.Servers, ","),
		// SASL 验证机制类型默认选用 PLAIN
		"sasl.mechanism": "PLAIN",
		// 在本地配置 ACL 策略。
		"security.protocol": "SASL_PLAINTEXT",
		// username 是实例 ID + # + 配置的用户名，password 是配置的用户密码。
		"sasl.username": fmt.Sprintf("%s#%s", cfg.Ckafka.InstanceId, cfg.Ckafka.Username),
		"sasl.password": cfg.Ckafka.Password,
		// 设置的消息消费组
		"group.id":          cfg.ConsumerGroupId,
		// auto.offset.reset参数定义了当无法获取消费分区的位移时从何处开始消费。例如:当Broker端没有offset(如第一次消费或offset超过7天过期)时
		// 如何初始化offset,当收到OFFSET_OUT_OF_RANGE 错误时如何重置 Offset。
		// auto.offset.reset 参数设置有如下选项：
		// earliest：表示自动重置到 partition 的最小 offset。
		// latest：默认为 latest，表示自动重置到 partition 的最大 offset。
		// none：不自动进行 offset 重置，抛出 OffsetOutOfRangeException 异常。
		// 详情参考：https://cloud.tencent.com/document/product/597/57572
		"auto.offset.reset": "latest",

		"enable.auto.commit":    false,// 是否自动提交
		"broker.address.family": "any",// 允许的broker IP地址族: any 所有,v4,v6
		"api.version.request":   true, // api版本请求

		// 使用 Kafka 消费分组机制时，消费者超时时间。当 Broker 在该时间内没有收到消费者的心跳时，认为该消费者故障失败，Broker
		// 发起重新 Rebalance 过程。目前该值的配置必须在 Broker 配置group.min.session.timeout.ms=6000和group.max.session.timeout.ms=300000 之间
		"session.timeout.ms": 10000,
		
	})

	if err != nil {
		log.Fatal(err)
	}
	// 订阅的消息topic 列表
	err = c.SubscribeTopics([]string{"test"}, nil)
	if err != nil {
		log.Fatal(err)
	}

	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
		} else {
			// 客户端将自动尝试恢复所有的 error
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}

	c.Close()
}
