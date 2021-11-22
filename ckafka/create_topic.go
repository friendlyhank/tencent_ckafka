package ckafka

import (
	ckafka "github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/ckafka/v20190819"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common"
	"github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common/profile"
	"log"
	config2 "tencent_ckafka/config"
)

// 创建topic
func createTopic(topic string) error{
	cfg, err := config2.ParseConfig("./config/config.json")
	if err != nil {
		log.Fatal(err)
	}
	credential :=common.NewCredential(
		cfg.Ckafka.KafkaAccessId,
		cfg.Ckafka.KafkaAccessSecret,
		)
	cpf := profile.NewClientProfile()
	cpf.HttpProfile.Endpoint = cfg.Ckafka.Endpoint
	client,err  := ckafka.NewClient(credential,cfg.Ckafka.Region,cpf)
	if err != nil{
		return err
	}

	request := ckafka.NewCreateTopicRequest()
	request.InstanceId = &cfg.Ckafka.InstanceId
	request.TopicName = &topic
	partitionNum := int64(1)
	replicaNum := int64(2)
	request.PartitionNum = &partitionNum
	request.ReplicaNum = &replicaNum

	response,err := client.CreateTopic(request)
	if err != nil{
		return err
	}

	log.Fatalf("%v",response)
	return nil
}
