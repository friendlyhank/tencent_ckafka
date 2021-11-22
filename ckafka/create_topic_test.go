package ckafka

import (
	"fmt"
	"testing"
)

func Test_create_topic(t *testing.T){
	err := createTopic("test")
	fmt.Println(err)
}
