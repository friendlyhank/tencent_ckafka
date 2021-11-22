package config

import (
	"encoding/json"
	"os"
)

// CKafkaConfig Ckafka配置项
type CKafkaConfig struct {
	Topic           []string   `json:"topic"`
	Ckafka            CKafkaInfo `json:"sasl"`
	Servers         []string   `json:"bootstrapServers"`
	ConsumerGroupId string     `json:"consumerGroupId"`
}

// CKafka配置
type CKafkaInfo struct {
	Username   string `json:"username"`
	Password   string `json:"password"`
	InstanceId string `json:"instanceId"`
	KafkaAccessId string `json:"kafka_access_id"`
	KafkaAccessSecret      string `json:"kafka_access_secret"`
	Endpoint               string `json:"endpoint"`
	Region                 string `json:"region"`
}

// ParseConfig 配置解析结构
func ParseConfig(configPath string) (*CKafkaConfig, error) {
	fileContent, err := os.Open(configPath)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(fileContent)
	c := &CKafkaConfig{}
	decodeError := decoder.Decode(c)
	if decodeError != nil {
		return nil, decodeError
	}
	return c, nil
}
