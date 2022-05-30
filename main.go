package main

import (
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

	_, _ = client.NewConsumer("consumer-1", consumeConfig1, func(delivery amqp.Delivery) error {
		delivery.Ack(true)
		return nil
	})

	_, _ = client.NewConsumer("consumer-2", consumeConfig2, func(delivery amqp.Delivery) error {
		delivery.Ack(true)
		return nil
	})

	defer client.Close()

	log.Printf("Wait for new messages, to exit press CTRL+C")
	<-forever
}
