package bunny

import (
	"github.com/streadway/amqp"
)

/*
Further documentation on exchanges, queues and bindings can be found in the rabbitMQ docks https://www.rabbitmq.com/tutorials/amqp-concepts.html
as well as this blog post https://www.cloudamqp.com/blog/part4-rabbitmq-for-beginners-exchanges-routing-keys-bindings.html.
The github.com/streadway/amqp library which the bunny library wraps also has relevant documentation.
Some of the language used in comments here is taken from these sources.
*/

// Exchange represents the parameters needed to declare an exchange
// https://www.rabbitmq.com/amqp-0-9-1-reference.html#exchange.declare
type Exchange struct {
	// Name of the exchange being declared
	Name string

	// The Type of exchange: direct, headers, fanout, topic
	Type string

	// Durable exchanges remain active when a server restarts. Non-durable exchanges (transient
	// exchanges) are purged if/when a server restarts
	Durable bool

	// When AutoDelete is enabled, the exchange is deleted when last queue/exchange is
	// unbound from it
	AutoDelete bool

	// If Internal is set, the exchange may not be used directly by publishers, but only when
	// bound to other exchanges. Internal exchanges are used to construct wiring that is not
	// visible to applications.
	Internal bool

	// If NoWait is set, the server will not respond to the method. The client should not wait
	// for a reply
	NoWait bool

	// Optional amqp.Table of arguments that are specific to the server's implementation of
	// the exchange can be sent for exchange types that require extra parameters
	Arguments amqp.Table
}

// Declare an exchange. Returns a SetupFunc which will be used to declare an exchange on
// startup and reconnect
func (e Exchange) Declare() SetupFunc {
	return func(ch *amqp.Channel) error {
		return ch.ExchangeDeclare(e.Name, e.Type, e.Durable, e.AutoDelete, e.Internal, e.NoWait, e.Arguments)
	}
}

// Bind will bind this exchange to another exchange, based on the
// configured parameters. Note that messages will flow to this exchange,
// from the configured parent exchange
func (e *Exchange) Bind(b Binding) SetupFunc {
	return func(ch *amqp.Channel) error {
		return ch.ExchangeBind(e.Name, b.RoutingKey, b.ParentExchange, b.NoWait, b.Arguments)
	}
}

// Queue represents the parameters needed to declare a queue
// https://www.rabbitmq.com/amqp-0-9-1-reference.html#queue.declare
type Queue struct {
	// Name of the queue being declared
	Name string

	// Durable queues remain active when a server restarts. Non-durable queues (transient
	// queues) are purged if/when a server restarts
	Durable bool

	// Exclusive queues may only be accessed by the current connection, and are deleted when
	// that connection closes.
	Exclusive bool

	// If AutoDelete set, the queue is deleted when all consumers have finished using it
	AutoDelete bool

	// If NoWait is set, the server will not respond to the method. The client should not wait
	// for a reply
	NoWait bool

	// Optional amqp.Table of arguments that are specific to the server's implementation of
	// the exchange can be sent for exchange types that require extra parameters
	Arguments amqp.Table
}

// Declare a queue. Returns a SetupFunc which will be used to declare a queue on
// startup and reconnect
func (q Queue) Declare() SetupFunc {
	return func(ch *amqp.Channel) error {
		if _, err := ch.QueueDeclare(
			q.Name, q.Durable, q.AutoDelete,
			q.Exclusive, q.NoWait, q.Arguments,
		); err != nil {
			return err
		}

		return nil
	}
}

// Bind will bind this queue to an exchange, based on the configured parameters
func (q *Queue) Bind(b Binding) SetupFunc {
	return func(ch *amqp.Channel) error {
		return ch.QueueBind(q.Name, b.RoutingKey, b.ParentExchange, b.NoWait, b.Arguments)
	}
}

// Binding holds the parameters necessary to bind a queue or an exchange to an exchange.
// https://www.rabbitmq.com/amqp-0-9-1-reference.html#exchange.bind
// https://www.rabbitmq.com/amqp-0-9-1-reference.html#queue.bind
type Binding struct {
	// ParentExchange is the name of the exchange that is being bound to. This is the exchange
	// where deliveries will be arriving from. It is also referred to as the "source" exchange
	// in some amqp client libraries. The RabbitMQ UI displays this as "from exchange"
	ParentExchange string

	// RoutingKey is a message attribute the exchange looks at when deciding how to route
	// the message to queues and exchanges (depending on exchange type)
	RoutingKey string

	// If NoWait is true, do not wait for the server to confirm the binding. If any
	// error occurs the channel will be closed
	NoWait bool

	// Arguments can optionally be passed to manage things such as headers when binding
	// to a headers exchange
	Arguments amqp.Table
}

// ExchangeToExchange will bind the given exchange to another exchange, based on the
// configured parameters. Not that messages will flow to the named exchange,
// from the configured parent exchange
func (b Binding) ExchangeToExchange(exchangeName string) SetupFunc {
	return func(ch *amqp.Channel) error {
		return ch.ExchangeBind(exchangeName, b.RoutingKey, b.ParentExchange, b.NoWait, b.Arguments)
	}
}

// QueueToExchange will bind the given queue to an exchange, based on the
// configured parameters
func (b Binding) QueueToExchange(queueName string) SetupFunc {
	return func(ch *amqp.Channel) error {
		return ch.QueueBind(queueName, b.RoutingKey, b.ParentExchange, b.NoWait, b.Arguments)
	}
}
