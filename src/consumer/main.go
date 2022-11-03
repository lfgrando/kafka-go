package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	consumer := GetKafkaConsumer()
	topics := []string{"topic-test"}

	consumer.SubscribeTopics(topics, nil)

	for {
		msg, err := consumer.ReadMessage(-1)
		if err == nil {
			fmt.Println(string(msg.Value), msg.TopicPartition)
		}
	}
}

func GetKafkaConsumer() *kafka.Consumer {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "kafka-server:9092",
		"client.id":         "goapp-consumer",
		"group.id":          "goapp-group-1",
		"auto.offset.reset": "earliest",
	}

	consumer, err := kafka.NewConsumer(configMap)

	if err != nil {
		log.Println(err.Error())
	}

	return consumer
}
