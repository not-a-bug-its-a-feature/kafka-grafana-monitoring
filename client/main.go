package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	kafka "github.com/segmentio/kafka-go"
)

func main() {
	brokers := strings.Split(os.Getenv("KAFKA_BROKERS"), ",")
	topic := os.Getenv("TOPIC_NAME")
	messageCount := 1000
	
	// Producer
	go func() {
		writer := kafka.NewWriter(kafka.WriterConfig{
			Brokers:	brokers,
			Topic:		topic,
			Balancer: 	&kafka.LeastBytes{},
			Async:		true,
		})

		defer writer.Close()

		for i := 0; i <= messageCount ; i++ {
			msg := fmt.Sprintf("Test Queue #%d", i)
			err := writer.WriteMessages(context.Background(),
				kafka.Message{
					Key:	[]byte(fmt.Sprintf("key-%d", i)),
					Value:	[]byte(msg),
				},
			)

			if err != nil {
				log.Printf("Fail Producer error: %v", err)
			} else {
				log.Printf("Success Produced: %s", msg)
			}
		}
	}()

	// Consumer
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:	brokers,
		Topic: 		topic,
		GroupID:	"test-group",
	})

	defer reader.Close()

	for {
		msg, err := reader.ReadMessage(context.Background())

		if err != nil {
			log.Printf("Fail Consumer error: %v", err)
			continue
		}

		log.Printf("Concumed: %s = %s", string(msg.Key), string(msg.Value))
	}
}