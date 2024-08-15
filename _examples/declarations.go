package _examples

import (
	"github.com/streadway/amqp"

	"github.com/Shimmur/bunny"
)

// set up a basic consumer by declaring an exchange and a queue, and binding
// that queue to the exchange
func basicConsumer() error {
	b := bunny.NewRabbit(bunny.ConnectionDetails{
		URLs: []string{"amqp://guest:guest@localhost:5672"},
	})

	// define an exchange
	myExchange := bunny.Exchange{
		Name:    "my_exchange",
		Type:    amqp.ExchangeFanout,
		Durable: true,
	}

	// define a queue
	myQueue := bunny.Queue{
		Name:       "my_queue",
		Durable:    true,
		AutoDelete: true,
	}

	// declare a new exchange and queue, and bind the queue to the exchange
	if _, err := b.NewConsumerChannel(
		myExchange.Declare(),
		myQueue.Declare(),
		myQueue.Bind(bunny.Binding{
			ParentExchange: "my_exchange",
			RoutingKey:     "#",
		}),
	); err != nil {
		return err
	}

	return nil
}

// declare a new queue to consume from an existing exchange
func consumeExistingExchange() error {
	b := bunny.NewRabbit(bunny.ConnectionDetails{
		URLs: []string{"amqp://guest:guest@localhost:5672"},
	})

	// set up a new consumer and bind exising queue to existing exchange
	if _, err := b.NewConsumerChannel(
		bunny.Queue{
			Name:       "my_queue",
			Durable:    true,
			AutoDelete: true,
		}.Declare(),
		bunny.Binding{
			ParentExchange: "existing_exchange",
			RoutingKey:     "#",
		}.QueueToExchange("my_queue"),
	); err != nil {
		return err
	}

	return nil
}

// declare an exchange and publish to it
func basicPublisher() error {
	b := bunny.NewRabbit(bunny.ConnectionDetails{
		URLs: []string{"amqp://guest:guest@localhost:5672"},
	})

	if _, err := b.NewPublisherChannel(
		bunny.PublishOptions{},

		// declare an exchange
		bunny.Exchange{
			Name:    "new_exchange",
			Type:    amqp.ExchangeDirect,
			Durable: true,
		}.Declare(),
	); err != nil {
		return err
	}

	return nil
}

// Define topology that will be declared when a connection is established.
// Use this method of declaring for things that are not specific to a single consumer.
// This example demonstrates setting up two exchanges and binding one to the other
func defineTopLevelTopology() error {
	b := bunny.NewRabbit(bunny.ConnectionDetails{
		URLs: []string{"amqp://guest:guest@localhost:5672"},
	})

	// define top level topology
	if err := b.DeclareTopology(
		bunny.Exchange{
			Name:    "main_exchange",
			Type:    amqp.ExchangeDirect,
			Durable: true,
		}.Declare(),
		bunny.Exchange{
			Name:    "secondary_exchange",
			Type:    amqp.ExchangeFanout,
			Durable: true,
		}.Declare(),
		bunny.Binding{
			ParentExchange: "main_exchange",
			RoutingKey:     "#",
		}.ExchangeToExchange("secondary_exchange"),
	); err != nil {
		return err
	}

	return nil
}

// add a custom func to use the streadway/amqp api directly to do something that is
// not supported by the bunny API, for example, delete an exchange
func deleteAnExchange() error {
	b := bunny.NewRabbit(bunny.ConnectionDetails{
		URLs: []string{"amqp://guest:guest@localhost:5672"},
	})

	if err := b.DeclareTopology(
		func(ch *amqp.Channel) error {
			return ch.ExchangeDelete("old_exchange", false, false)
		},
		// declare a new exchange to replace the deleted one
		bunny.Exchange{
			Name: "replacement_exchange",
			Type: amqp.ExchangeFanout,
		}.Declare(),
	); err != nil {
		return err
	}

	return nil
}
