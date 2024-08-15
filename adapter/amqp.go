package adapter

// This package exists because the streadway/amqp library exposes structs directly rather than
// interfaces. As revealed by the issue linked below, the author has no intent of altering the
// library to include interfaces, and suggests writing wrappers that could be mocked. Thus this
// package does exactly that.
// https://github.com/streadway/amqp/issues/64

// Some other references and examples of working around this issue
// https://www.reddit.com/r/golang/comments/2pwobl/how_to_unit_test_code_working_with_rabbitmq/
// https://gist.github.com/leehambley/e688257f1fa0451eec8a

import (
	"crypto/tls"

	"github.com/streadway/amqp"
)

/*--------
| Dialer |
--------*/

//go:generate counterfeiter -o ../fakes/fake_AMQPDialer.go . AMQPDialer

// AMQPDialer interface that allows for mocking of the connection
type AMQPDialer interface {
	Dial(url string, tlsConfig *tls.Config) (AMQPConnection, error)
}

type Dialer struct{}

func (d *Dialer) Dial(url string, tlsConfig *tls.Config) (AMQPConnection, error) {
	var (
		ac  *amqp.Connection
		err error
	)

	if tlsConfig == nil {
		ac, err = amqp.Dial(url)
	} else {
		ac, err = amqp.DialTLS(url, tlsConfig)
	}

	if err != nil {
		return nil, err
	}

	return &connWrapper{
		Connection: ac,
	}, nil
}

/*------------
| Connection |
------------*/

//go:generate counterfeiter -o ../fakes/fake_amqpConnection.go . AMQPConnection

// AMQPConnection interface of the amqp.Connection methods that we use, so it can be mocked
type AMQPConnection interface {
	NotifyClose(receiver chan *amqp.Error) chan *amqp.Error
	Close() error
	Channel() (AMQPChannel, error)
}

type connWrapper struct {
	*amqp.Connection
}

func (w *connWrapper) Channel() (AMQPChannel, error) {
	return w.Connection.Channel()
}

/*---------
| Channel |
---------*/

//go:generate counterfeiter -o ../fakes/fake_amqpChannel.go . AMQPChannel

// AMQPChannel interface of the amqp.Channel methods that we use, so it can be mocked
type AMQPChannel interface {
	Close() error
	Cancel(consumer string, noWait bool) error
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	Qos(prefetchCount, prefetchSize int, global bool) error
}

func UnwrapChannel(channel AMQPChannel) *amqp.Channel {
	ch, ok := channel.(*amqp.Channel)
	if ok {
		return ch
	}

	return nil
}
