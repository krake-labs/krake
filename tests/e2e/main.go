package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const brokerAddress = "localhost:9092"

func main() {
	var topic = "test"

	// Set up a Kafka producer to produce messages.
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": brokerAddress,
	})
	if err != nil {
		log.Fatalf("Error initializing producer: %s", err)
	}
	defer producer.Close()

	// Produce a Kafka message.
	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: []byte("Hello, world!"),
	}

	err = producer.Produce(message, nil)
	if err != nil {
		log.Fatalf("Error producing message: %s", err)
	}

	fmt.Println("Message sent.")

	// Set up a Kafka consumer to consume messages.
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": brokerAddress,
		"group.id":          "my-group",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatalf("Error initializing consumer: %s", err)
	}
	defer consumer.Close()

	// Subscribe to the Kafka topic.
	err = consumer.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		log.Fatalf("Error subscribing to topic: %s", err)
	}

	// Set up a signal handler to gracefully shut down the program.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// Continuously consume messages until the program is interrupted.
	for {
		select {
		case <-signals:
			fmt.Println("Interrupt signal received, shutting down.")
			return
		default:
			message, err := consumer.ReadMessage(-1)
			if err != nil {
				log.Fatalf("Error reading message: %s", err)
			}

			fmt.Printf("Message received: partition=%d, offset=%d, value=%s\n", message.TopicPartition.Partition, message.TopicPartition.Offset, string(message.Value))
		}
	}
}
