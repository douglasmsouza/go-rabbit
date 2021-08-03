package rabbitmq

import (
	"github.com/streadway/amqp"
)

type ExchangeType string

const (
	Direct  ExchangeType = "direct"
	Fanout  ExchangeType = "fanout"
	Topic   ExchangeType = "topic"
	Headers ExchangeType = "headers"
)

type Exchange struct {
	Name       string
	Type       ExchangeType
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       amqp.Table
}

type Queue struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       amqp.Table
}

type Binding struct {
	RoutingKey string
	NoWait     bool
	Args       amqp.Table
}

type Listener struct {
	Tag       string
	AutoAck   bool
	Exclusive bool
	NoLocal   bool
	NoWait    bool
	Args      amqp.Table
}

type ConsumeConfig struct {
	Queue    Queue
	Exchange *Exchange
	Binding  *Binding
	Listener Listener
}

type RabbitConsumer interface {
	consume(f func(delivery amqp.Delivery)) error
	Close() error
}

type rabbitConsumerImpl struct {
	channel *amqp.Channel
	config  ConsumeConfig
}

func (r rabbitConsumerImpl) consume(f func(delivery amqp.Delivery)) error {
	queue, err := r.channel.QueueDeclare(
		r.config.Queue.Name,
		r.config.Queue.Durable,
		r.config.Queue.AutoDelete,
		r.config.Queue.Exclusive,
		r.config.Queue.NoWait,
		r.config.Queue.Args,
	)
	if err != nil {
		return err
	}

	if r.config.Exchange != nil {
		err = r.channel.ExchangeDeclare(
			r.config.Exchange.Name,
			string(r.config.Exchange.Type),
			r.config.Exchange.Durable,
			r.config.Exchange.AutoDelete,
			r.config.Exchange.Internal,
			r.config.Exchange.NoWait,
			r.config.Exchange.Args,
		)
		if err != nil {
			return err
		}

		if r.config.Binding != nil {
			err = r.channel.QueueBind(
				queue.Name,
				r.config.Binding.RoutingKey,
				r.config.Exchange.Name,
				r.config.Binding.NoWait,
				r.config.Binding.Args,
			)
			if err != nil {
				return err
			}
		}
	}

	deliveries, err := r.channel.Consume(
		queue.Name,
		r.config.Listener.Tag,
		r.config.Listener.AutoAck,
		r.config.Listener.Exclusive,
		r.config.Listener.NoLocal,
		r.config.Listener.NoWait,
		r.config.Listener.Args,
	)
	if err != nil {
		return err
	}

	go func() {
		for delivery := range deliveries {
			f(delivery)
		}
	}()

	return nil
}

func (r rabbitConsumerImpl) Close() error {
	return r.channel.Close()
}
