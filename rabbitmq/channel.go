package rabbitmq

import "github.com/streadway/amqp"

type hasChannel interface {
	Close() error
	getName() string
	getChannel() *amqp.Channel
	updateChannel(channel *amqp.Channel)
}
