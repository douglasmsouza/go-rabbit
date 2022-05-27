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

type ConsumeConfig struct {
	Queue     Queue
	Exchange  *Exchange
	Binding   Binding
	AutoAck   bool
	Exclusive bool
	NoLocal   bool
	NoWait    bool
	Args      amqp.Table
}

type RabbitConsumer interface {
	Close() error
}

type rabbitConsumerImpl struct {
	name    string
	channel *amqp.Channel
	config  ConsumeConfig
	handler func(delivery amqp.Delivery)
}

func (r *rabbitConsumerImpl) bindQueue() (*amqp.Queue, error) {
	queue, err := r.channel.QueueDeclare(
		r.config.Queue.Name,
		r.config.Queue.Durable,
		r.config.Queue.AutoDelete,
		r.config.Queue.Exclusive,
		r.config.Queue.NoWait,
		r.config.Queue.Args,
	)
	if err != nil {
		return nil, err
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
			return nil, err
		}

		err = r.channel.QueueBind(
			queue.Name,
			r.config.Binding.RoutingKey,
			r.config.Exchange.Name,
			r.config.Binding.NoWait,
			r.config.Binding.Args,
		)
		if err != nil {
			return nil, err
		}
	}

	return &queue, nil
}

func (r *rabbitConsumerImpl) start() error {
	queue, err := r.bindQueue()
	if err != nil {
		return err
	}

	deliveries, err := r.channel.Consume(
		queue.Name,
		r.name,
		r.config.AutoAck,
		r.config.Exclusive,
		r.config.NoLocal,
		r.config.NoWait,
		r.config.Args,
	)
	if err != nil {
		return err
	}

	go func() {
		for delivery := range deliveries {
			r.handler(delivery)
		}
	}()

	return nil
}

func (r *rabbitConsumerImpl) Close() error {
	return r.channel.Close()
}

func (r *rabbitConsumerImpl) updateChannel(channel *amqp.Channel) {
	r.channel = channel
	r.start()
}

func (r *rabbitConsumerImpl) getChannel() *amqp.Channel {
	return r.channel
}

func (r *rabbitConsumerImpl) getName() string {
	return r.name
}
