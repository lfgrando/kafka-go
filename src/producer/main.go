package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	deliveryChan := make(chan kafka.Event)
	producer := GetKafkaProducer()

	key := []byte(nil)

	// Use the same custom key to send all messages to the same partition.
	// key := []byte("operation-key")

	for i := 1; i < 10; i++ {
		PublishMessage(fmt.Sprintf("message: %b", i), "topic-test", producer, key, deliveryChan)
	}

	// Async mode.
	ReportMessageDelivery(deliveryChan)

	// Sync mode.
	//e := <-deliveryChan
	//msg := e.(*kafka.Message)
	//if msg.TopicPartition.Error != nil {
	//	fmt.Println("Erro ao enviar")
	//} else {
	//	fmt.Println("Mensagem enviada:", msg.TopicPartition)
	//}
	//

}

func GetKafkaProducer() *kafka.Producer {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers":   "kafka-server:9092",
		"delivery.timeout.ms": "0",
		"acks":                "all",
		"enable.idempotence":  "true",
	}

	producer, err := kafka.NewProducer(configMap)

	if err != nil {
		log.Println(err.Error())
	}

	return producer
}

func PublishMessage(msg string, topic string, producer *kafka.Producer, key []byte, deliveryChan chan kafka.Event) error {
	message := &kafka.Message{
		Value:          []byte(msg),
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            key,
	}

	err := producer.Produce(message, deliveryChan)

	if err != nil {
		return err
	}

	return nil
}

func ReportMessageDelivery(deliveryChan chan kafka.Event) {
	for e := range deliveryChan {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				fmt.Println("Failed to send. Error: " + ev.TopicPartition.Error.Error())
			} else {
				fmt.Println("Message sent to partition:", ev.TopicPartition)

				// Business logic should to be continued here.
			}
		}
	}
}
