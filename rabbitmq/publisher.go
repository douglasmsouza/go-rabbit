package rabbitmq

import (
	"bytes"
	"encoding/json"

	"github.com/streadway/amqp"
)

type PublishConfig struct {
	Exchange   string
	RoutingKey string
	Mandatory  bool
	Immediate  bool
}

type RabbitPublisher struct {
	channel *amqp.Channel
	config  PublishConfig
}

func (r RabbitPublisher) Publish(p amqp.Publishing) error {
	err := r.channel.Publish(
		r.config.Exchange,
		r.config.RoutingKey,
		r.config.Mandatory,
		r.config.Immediate,
		p,
	)

	return err
}

func (r RabbitPublisher) PublishJSON(v interface{}, p amqp.Publishing) error {
	buffer := &bytes.Buffer{}
	encoder := json.NewEncoder(buffer)
	encoder.SetEscapeHTML(false)

	err := encoder.Encode(v)
	if err != nil {
		return err
	}

	p.ContentType = "application/json"
	p.Body = buffer.Bytes()
	if p.DeliveryMode == 0 {
		p.DeliveryMode = amqp.Persistent
	}

	err = r.Publish(p)
	return err
}
