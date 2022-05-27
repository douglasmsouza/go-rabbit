package main

import (
	"fmt"
	"log"

	"github.com/douglasmsouza/go-rabbit/rabbitmq"
	"github.com/koding/logging"
	"github.com/streadway/amqp"
)

func main() {
	rabbitConfig := rabbitmq.NewRabbitConfig("localhost", 5672, "guest", "guest", logging.DEBUG)
	client := rabbitmq.NewRabbitClient(rabbitConfig)

	consumeConfig1 := rabbitmq.ConsumeConfig{
		Queue: rabbitmq.Queue{
			Name:    "test1",
			Durable: true,
		},
		Exchange: &rabbitmq.Exchange{
			Name:    "test",
			Durable: true,
			Type:    rabbitmq.Fanout,
		},
	}

	consumeConfig2 := rabbitmq.ConsumeConfig{
		Queue: rabbitmq.Queue{
			Name:    "test2",
			Durable: true,
		},
		Exchange: &rabbitmq.Exchange{
			Name:    "test",
			Durable: true,
			Type:    rabbitmq.Fanout,
		},
	}

	forever := make(chan bool)

	_, _ = client.NewConsumer("consumer-1", consumeConfig1, func(delivery amqp.Delivery) {
		fmt.Printf("%s", string(delivery.Body))
		fmt.Println()

		delivery.Ack(true)
	})

	_, _ = client.NewConsumer("consumer-2", consumeConfig2, func(delivery amqp.Delivery) {
		fmt.Printf("%s", string(delivery.Body))
		fmt.Println()

		delivery.Ack(true)
	})

	defer client.Close()

	log.Printf("Wait for new messages, to exit press CTRL+C")
	<-forever
}
