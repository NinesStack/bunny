# Bunny
A Golang client library for RabbitMQ. It wraps the [streadway/amqp](https://github.com/streadway/amqp)
library to improve ergonamics. It also provides missing functionality such as automatic reconnects, when
a connection to RabbitMQ is lost.

## Usage

Below is a basic example of usage. See [examples](_examples) for more detailed usage.

```go
package main

import (
	"log"
	"time"

	"github.com/Shimmur/bunny"
	"github.com/streadway/amqp"
)

func main() {
	b := bunny.NewRabbit(bunny.ConnectionDetails{
		URLs: []string{"amqp://guest:guest@localhost:5672"},
	})

	/*-----------
	| Consuming |
	-----------*/

	// define an exchange
	myExchange := bunny.Exchange{
		Name:    "my_exchange",
		Type:    amqp.ExchangeFanout,
		Durable: true,
	}

	// define a queue
	myQueue:= bunny.Queue{
		Name:    "my_queue",
		Durable: true,
	}

	// declare a new exchange and queue, and bind the queue to the exchange
	c, err := b.NewConsumerChannel(
		myExchange.Declare(),
		myQueue.Declare(),
		myQueue.Bind(bunny.Binding{
			ParentExchange: "my_exchange",
			RoutingKey:     "#",
		}),
	)
	if err != nil {
		log.Fatal(err)
	}

	// function to handle each delivery on the consumer channel
	consumeFunc := func(msg *amqp.Delivery) error {
		// consume logic here
		return nil
	}

	if err = c.Consume(
		consumeFunc,
		bunny.ConsumeOptions{
			QueueName: "my_queue",
			//optionally set qos
			QoSOptions: bunny.QoSOptions{
				PrefetchCount: 100,
			},
        },
		nil,
	); err != nil {
		log.Fatal(err)
	}

	/*------------
	| Publishing |
	------------*/

	pc, err := b.NewPublisherChannel(
		bunny.PublishOptions{},
        bunny.Exchange{
            Name:    "new_exchange",
            Type:    amqp.ExchangeDirect,
            Durable: true,
        }.Declare(),
	)
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		if err := pc.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	msg := amqp.Publishing{
		ContentType: "text/plan",
		Body:        []byte(`whatever`),
	}

	// publish
	if err = pc.Publish(msg); err != nil {
		log.Fatal(err)
	}

	// publish with retries
	if err = pc.PublishWithRetires(msg, []time.Duration{
		time.Millisecond * 2,
	}); err != nil {
		log.Fatal(err)
	}
}
```
