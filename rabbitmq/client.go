package rabbitmq

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/koding/logging"
	"github.com/streadway/amqp"
)

type RabbitConfig struct {
	host     string
	port     int
	username string
	password string
	logLevel logging.Level
}

func NewRabbitConfig(host string, port int, username, password string, logLevel logging.Level) RabbitConfig {
	return RabbitConfig{
		host:     host,
		port:     port,
		username: username,
		password: password,
		logLevel: logLevel,
	}
}

type RabbitClient interface {
	Close()
	NewPublisher(name string, config PublishConfig) (RabbitPublisher, error)
	NewConsumer(name string, config ConsumeConfig, f func(delivery amqp.Delivery)) (RabbitConsumer, error)
}

type rabbitClientImpl struct {
	config    RabbitConfig
	conn      *amqp.Connection
	connMutex sync.Mutex
	logger    logging.Logger
}

func NewRabbitClient(config RabbitConfig) RabbitClient {
	logHandler := logging.NewWriterHandler(os.Stderr)
	logHandler.SetLevel(config.logLevel)

	logger := logging.NewLogger("rabbitmq")
	logger.SetLevel(config.logLevel)
	logger.SetHandler(logHandler)

	return &rabbitClientImpl{
		config: config,
		logger: logger,
	}
}

func (r *rabbitClientImpl) newChannel() (*amqp.Channel, error) {
	r.connMutex.Lock()
	defer r.connMutex.Unlock()

	if r.conn == nil || r.conn.IsClosed() {
		url := fmt.Sprintf("amqp://%s:%s@%s:%d/", r.config.username, r.config.password, r.config.host, r.config.port)
		c, err := amqp.Dial(url)
		if err != nil {
			return nil, err
		}

		r.conn = c
	}

	channel, err := r.conn.Channel()
	return channel, err
}

func (r *rabbitClientImpl) Close() {
	if r.conn != nil {
		_ = r.conn.Close()
	}
}

func (r *rabbitClientImpl) NewPublisher(name string, config PublishConfig) (RabbitPublisher, error) {
	channel, err := r.newChannel()
	if err != nil {
		return nil, err
	}

	p := &rabbitPublisherImpl{
		name:    name,
		channel: channel,
		config:  config,
	}

	r.handleReconnection(p)

	r.logger.Debug("new publisher started: %s", p.name)

	return p, nil
}

func (r *rabbitClientImpl) NewConsumer(name string, config ConsumeConfig, handler func(delivery amqp.Delivery)) (RabbitConsumer, error) {
	channel, err := r.newChannel()
	if err != nil {
		return nil, err
	}

	c := &rabbitConsumerImpl{
		name:    name,
		channel: channel,
		config:  config,
		handler: handler,
	}

	r.handleReconnection(c)
	c.start()

	r.logger.Debug("new consumer started: %s", c.name)

	return c, nil
}

func (r *rabbitClientImpl) handleReconnection(hasChannel hasChannel) {
	go func() {
		<-hasChannel.getChannel().NotifyClose(make(chan *amqp.Error))

		r.logger.Debug("trying to reconnect channel %s", hasChannel.getName())

		for {
			newChannel, err := r.newChannel()
			if err != nil {
				r.logger.Debug("connection not available, will retry...")
				time.Sleep(10 * time.Second)
			} else {
				r.logger.Debug("new channel created")
				_ = hasChannel.Close()
				hasChannel.updateChannel(newChannel)
				r.logger.Debug("new channel updated")
				break
			}
		}
	}()
}
