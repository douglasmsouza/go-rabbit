package rabbitmq

import (
	"crypto/tls"
	"fmt"
	"sync"

	"github.com/koding/logging"
	"github.com/streadway/amqp"
)

const LOG_NAME = "rabbit-client"

type RabbitConfig struct {
	host     string
	port     int
	username string
	password string
	logLevel logging.Level
	ssl      bool
}

func (r RabbitConfig) protocol() string {
	if r.ssl {
		return "amqps"
	}
	return "amqp"
}

func (r RabbitConfig) tlsClientConfig() *tls.Config {
	if r.ssl {
		return &tls.Config{
			InsecureSkipVerify: true,
		}
	}
	return nil
}

func NewRabbitConfig(host string, port int, username, password string, logLevel logging.Level) RabbitConfig {
	return RabbitConfig{
		host:     host,
		port:     port,
		username: username,
		password: password,
		logLevel: logLevel,
		ssl:      false,
	}
}

func NewRabbitConfigWithSSL(host string, port int, username, password string, logLevel logging.Level) RabbitConfig {
	return RabbitConfig{
		host:     host,
		port:     port,
		username: username,
		password: password,
		logLevel: logLevel,
		ssl:      true,
	}
}

type RabbitClient interface {
	Close()
	NewPublisher(name string, config PublishConfig) (RabbitPublisher, error)
	NewConsumer(name string, config ConsumeConfig, f DeliveryHandler) (RabbitConsumer, error)
}

type rabbitClientImpl struct {
	config    RabbitConfig
	conn      *amqp.Connection
	connMutex sync.Mutex
	logger    logging.Logger
	channels  []rabbitChannel
	name      string
}

func NewRabbitClient(config RabbitConfig) RabbitClient {
	return NewRabbitClientWithName(config, "")
}

func NewRabbitClientWithName(config RabbitConfig, name string) RabbitClient {
	return &rabbitClientImpl{
		config:   config,
		logger:   newLogger(LOG_NAME, config.logLevel),
		channels: []rabbitChannel{},
		name:     name,
	}
}

func (r *rabbitClientImpl) newChannel() (*amqp.Channel, error) {
	r.connMutex.Lock()
	defer r.connMutex.Unlock()

	if r.conn == nil || r.conn.IsClosed() {
		url := fmt.Sprintf("%s://%s:%s@%s:%d/", r.config.protocol(), r.config.username, r.config.password, r.config.host, r.config.port)
		c, err := amqp.DialConfig(url, amqp.Config{
			Properties: amqp.Table{
				"connection_name": r.name,
			},
			TLSClientConfig: r.config.tlsClientConfig(),
		})
		if err != nil {
			return nil, err
		}

		r.conn = c
	}

	channel, err := r.conn.Channel()
	return channel, err
}

func (r *rabbitClientImpl) Close() {
	for _, c := range r.channels {
		c.Close()
	}

	if r.conn != nil {
		_ = r.conn.Close()
	}

	r.channels = []rabbitChannel{}
}

func (r *rabbitClientImpl) newRabbitChannel(name string) (*rabbitChannel, error) {
	logger := newLogger(fmt.Sprintf("%s:%s", LOG_NAME, name), r.config.logLevel)
	channel, err := newRabbitChannel(logger, r.newChannel)
	if err != nil {
		return nil, err
	}
	r.appendChannels(*channel)
	return channel, nil
}

func (r *rabbitClientImpl) NewPublisher(name string, config PublishConfig) (RabbitPublisher, error) {
	channel, err := r.newRabbitChannel(name)
	if err != nil {
		return nil, err
	}

	p := newRabbitPublisher(*channel, config)
	r.logger.Debug("new publisher started: %s", name)
	return p, nil
}

func (r *rabbitClientImpl) NewConsumer(name string, config ConsumeConfig, handler DeliveryHandler) (RabbitConsumer, error) {
	channel, err := r.newRabbitChannel(name)
	if err != nil {
		return nil, err
	}

	c := newRabbitConsumer(*channel, config, handler)
	r.logger.Debug("new consumer started: %s", name)
	return c, nil
}

func (r *rabbitClientImpl) appendChannels(c rabbitChannel) {
	r.channels = append(r.channels, c)
}
