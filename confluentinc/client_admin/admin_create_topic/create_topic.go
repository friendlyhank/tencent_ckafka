// Create topic
package main

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"gokafkademo/config"
	"log"
	"os"
	"strings"
	"time"
)

func main() {

	cfg, err := config.ParseConfig("./config/kafka.json")
	if err != nil {
		log.Fatal(err)
	}

	// Create a new AdminClient.
	// AdminClient can also be instantiated using an existing
	// Producer or Consumer instance, see NewAdminClientFromProducer and
	// NewAdminClientFromConsumer.
	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{// 设置接入点，请通过控制台获取对应Topic的接入点。
		// 设置接入点，请通过控制台获取对应Topic的接入点。
		"bootstrap.servers": strings.Join(cfg.Servers, ","),
		// SASL 验证机制类型默认选用 PLAIN
		"sasl.mechanism": "PLAIN",
		// 在本地配置 ACL 策略。
		"security.protocol": "SASL_PLAINTEXT",
		// username 是实例 ID + # + 配置的用户名，password 是配置的用户密码。
		"sasl.username": fmt.Sprintf("%s#%s", cfg.SASL.InstanceId, cfg.SASL.Username),
		"sasl.password": cfg.SASL.Password,
		},
	)
	if err != nil {
		fmt.Printf("Failed to create Admin client: %s\n", err)
		os.Exit(1)
	}

	// Contexts are used to abort or limit the amount of time
	// the Admin call blocks waiting for a result.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create topics on cluster.
	// Set Admin options to wait for the operation to finish (or at most 60s)
	maxDur, err := time.ParseDuration("60s")
	if err != nil {
		panic("ParseDuration(60s)")
	}
	results, err := adminClient.CreateTopics(
		ctx,
		// Multiple topics can be created simultaneously
		// by providing more TopicSpecification structs here.
		[]kafka.TopicSpecification{{
			Topic:             "newtest",
			NumPartitions:     3,
			ReplicationFactor: 3}},
		// Admin options
		kafka.SetAdminOperationTimeout(maxDur))
	if err != nil {
		fmt.Printf("Failed to create topic: %v\n", err)
		os.Exit(1)
	}

	// Print results
	for _, result := range results {
		fmt.Printf("%s\n", result)
	}

	adminClient.Close()
}
