package main

import (
	"fmt"
	"log"

	"github.com/douglasmsouza/go-rabbit/rabbitmq"
	"github.com/streadway/amqp"
)

func main() {
	rabbitConfig := rabbitmq.NewRabbitConfig("localhost", 5672, "guest", "guest")
	rabbitClient := rabbitmq.NewRabbitClient(rabbitConfig)

	consumeConfig := rabbitmq.ConsumeConfig{
		Queue: rabbitmq.Queue{
			Name:    "test",
			Durable: true,
		},
		Exchange: &rabbitmq.Exchange{
			Name:    "test",
			Durable: true,
			Type:    rabbitmq.Topic,
		},
		Binding: &rabbitmq.Binding{
			RoutingKey: "test.message.create",
		},
	}

	forever := make(chan bool)

	consumer, err := rabbitClient.NewConsumer(consumeConfig, func(delivery amqp.Delivery) {
		fmt.Printf("%s", string(delivery.Body))
		fmt.Println()

		delivery.Ack(true)
	})
	if err != nil {
		panic(err.Error())
	}
	defer consumer.Close()

	log.Printf("Wait for new messages, to exit press CTRL+C")
	<-forever
}
