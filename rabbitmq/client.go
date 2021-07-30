package rabbitmq

import (
	"fmt"

	"github.com/streadway/amqp"
)

type RabbitConfig struct {
	Host     string
	Port     int
	Username string
	Password string
}

func NewRabbitConfig(host string, port int, username, password string) RabbitConfig {
	return RabbitConfig{
		Host:     host,
		Port:     port,
		Username: username,
		Password: password,
	}
}

type RabbitClient interface {
	Connection() (*amqp.Connection, error)
	Close() error
	NewPublisher(config PublishConfig) (*RabbitPublisher, error)
}

type rabbitClientImpl struct {
	conn   *amqp.Connection
	config RabbitConfig
}

func NewRabbitClient(config RabbitConfig) RabbitClient {
	return rabbitClientImpl{
		config: config,
	}
}

func (r rabbitClientImpl) Connection() (*amqp.Connection, error) {
	if r.conn == nil || r.conn.IsClosed() {
		url := fmt.Sprintf("amqp://%s:%s@%s:%d/", r.config.Username, r.config.Password, r.config.Host, r.config.Port)
		c, err := amqp.Dial(url)
		if err != nil {
			return nil, err
		}

		r.conn = c
	}

	return r.conn, nil
}

func (r rabbitClientImpl) Close() error {
	if r.conn == nil {
		return nil
	}

	err := r.conn.Close()
	return err
}

func (r rabbitClientImpl) NewPublisher(config PublishConfig) (*RabbitPublisher, error) {
	conn, err := r.Connection()
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	p := &RabbitPublisher{
		channel: ch,
		config:  config,
	}
	return p, nil
}
