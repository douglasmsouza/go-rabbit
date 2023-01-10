package rabbitmq

import (
	"time"

	"github.com/koding/logging"
	"github.com/streadway/amqp"
)

type channelSupplier = func() (*amqp.Channel, error)
type afterChannelReconnect = func()
type hasChannel interface {
	newChannel() (*amqp.Channel, error)
	getChannel() *amqp.Channel
	updateChannel(channel *amqp.Channel)
	logger() logging.Logger
}

type rabbitChannel struct {
	channel         *amqp.Channel
	log             logging.Logger
	channelSupplier channelSupplier
}

func newRabbitChannel(logger logging.Logger, channelSupplier channelSupplier) (*rabbitChannel, error) {
	channel, err := channelSupplier()
	if err != nil {
		return nil, err
	}

	r := &rabbitChannel{
		log:             logger,
		channel:         channel,
		channelSupplier: channelSupplier,
	}

	return r, nil
}

func (r *rabbitChannel) Close() error {
	return r.channel.Close()
}

func (r *rabbitChannel) getChannel() *amqp.Channel {
	return r.channel
}

func (r *rabbitChannel) logger() logging.Logger {
	return r.log
}

func (r *rabbitChannel) newChannel() (*amqp.Channel, error) {
	return r.channelSupplier()
}

func handleReconnection(h hasChannel) {
	go func() {
		err := <-h.getChannel().NotifyClose(make(chan *amqp.Error))

		if err == nil {
			return
		}

		for {
			h.logger().Debug("trying to reconnect channel with err: %d - %s", err.Code, err.Reason)

			newChannel, err := h.newChannel()
			if err != nil {
				h.logger().Debug("connection not available, will retry...")
				time.Sleep(10 * time.Second)
			} else {
				h.logger().Debug("new channel created")
				h.updateChannel(newChannel)
				h.logger().Debug("new channel updated")

				handleReconnection(h)

				break
			}
		}
	}()
}
