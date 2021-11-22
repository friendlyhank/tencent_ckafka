package ckafka

import (
	"fmt"
	"testing"
)

func Test_Create_Topic(t *testing.T){
	err := CreateTopic("test")
	fmt.Println(err)
}

func Test_DeleteTopic(t *testing.T){
	err := DeleteTopic("test")
	fmt.Println(err)
}
