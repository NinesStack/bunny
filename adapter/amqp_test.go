package adapter

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/streadway/amqp"
)

/*----------
| Wrappers |
----------*/

func TestWrappers(t *testing.T) {
	Convey("the structs meet our wrapper interfaces ", t, func() {
		var _ AMQPConnection = &connWrapper{}
		var _ AMQPChannel = &amqp.Channel{}
	})
}

func Test_UnwrapChannel(t *testing.T) {
	Convey("UnwrapChannel", t, func() {
		Convey("returns channel if it is wrapped correctly", func() {
			origCh := &amqp.Channel{}
			wrapped := AMQPChannel(origCh)

			ch := UnwrapChannel(wrapped)
			So(ch, ShouldNotBeNil)
			So(ch, ShouldEqual, origCh)
		})

		Convey("returns nil if not a proper channel", func() {
			wrapped := &notAChannel{}

			ch := UnwrapChannel(wrapped)
			So(ch, ShouldBeNil)
		})
	})
}

type notAChannel struct{}

func (n *notAChannel) Close() error {
	panic("test struct")
}

func (n *notAChannel) Cancel(consumer string, noWait bool) error {
	panic("test struct")
}

func (n *notAChannel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	panic("test struct")
}

func (n *notAChannel) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	panic("test struct")
}

func (n *notAChannel) Qos(prefetchCount, prefetchSize int, global bool) error {
	panic("test struct")
}
