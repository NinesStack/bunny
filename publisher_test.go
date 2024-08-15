package bunny

import (
	"errors"
	"fmt"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/streadway/amqp"

	"github.com/Shimmur/bunny/fakes"
)

func Test_NewPublisherChannel(t *testing.T) {
	Convey("NewPublisherChannel", t, func() {
		bunn := NewRabbit(ConnectionDetails{
			URLs: []string{"foobar"},
		})

		testOpts := PublishOptions{
			ExchangeName: "the_exchange",
			RoutingKey:   "route_me",
			Mandatory:    false,
			Immediate:    false,
		}

		fakeDialer := &fakes.FakeAMQPDialer{}
		fakeConn := &fakes.FakeAMQPConnection{}
		fakeDialer.DialReturns(fakeConn, nil)

		bunn.connDetails.dialer = fakeDialer

		err := bunn.Connect()
		So(err, ShouldBeNil)

		setup := func(ch *amqp.Channel) error { return nil }

		Convey("creates a new publisher channel and calls the setup func", func() {
			setupCalled := false
			setup := func(ch *amqp.Channel) error { setupCalled = true; return nil }

			c, err := bunn.NewPublisherChannel(testOpts, setup)
			So(err, ShouldBeNil)
			So(setupCalled, ShouldBeTrue)
			So(c, ShouldNotBeNil)
			pub, ok := c.(*publisher)
			So(ok, ShouldBeTrue)
			So(pub.chanSetupFuncs[0], ShouldEqual, setup)
			So(pub.id, ShouldNotBeEmpty)
			So(pub.opts, ShouldNotBeNil)
			So(pub.chanMux, ShouldNotBeNil)
			So(*pub.status, ShouldEqual, statusActive)
			So(pub.rmCallback, ShouldNotBeNil)

		})

		Convey("errors if setup func fails", func() {
			setup := func(ch *amqp.Channel) error { return errors.New("kaboom") }

			p, err := bunn.NewPublisherChannel(testOpts, setup)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "failed to setup channel with provided func")
			So(p, ShouldBeNil)
		})

		Convey("errors if there is no connection", func() {
			bunn.connections = nil

			_, err := bunn.NewPublisherChannel(testOpts, setup)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "no connection! Must call Connect()")
		})
	})
}

func Test_publisher_Publish(t *testing.T) {
	Convey("Publish", t, func() {
		active := statusActive
		fakeChan := &fakes.FakeAMQPChannel{}
		fakeMux := &fakes.FakeRwLocker{}

		opts := &PublishOptions{
			ExchangeName: "the_exchange",
			RoutingKey:   "route_me",
			Mandatory:    false,
			Immediate:    false,
		}

		pub := &publisher{
			id:       "fooID",
			status:   &active,
			opts:     opts,
			amqpChan: fakeChan,
			chanMux:  fakeMux,
		}

		testBody := "this is a test"
		testMsg := amqp.Publishing{
			ContentType: "test/plain",
			Body:        []byte(testBody),
		}

		Convey("should publish body to channel", func() {
			err := pub.Publish(testMsg)
			So(err, ShouldBeNil)

			// locks the mux
			So(fakeMux.RLockCallCount(), ShouldEqual, 1)
			So(fakeMux.RUnlockCallCount(), ShouldEqual, 1)

			// calls publish
			So(fakeChan.PublishCallCount(), ShouldEqual, 1)
			exchange, routingKey, mandatory, immediate, msg := fakeChan.PublishArgsForCall(0)
			So(exchange, ShouldEqual, opts.ExchangeName)
			So(routingKey, ShouldEqual, opts.RoutingKey)
			So(mandatory, ShouldEqual, opts.Mandatory)
			So(immediate, ShouldEqual, opts.Immediate)
			So(msg, ShouldResemble, testMsg)
		})

		Convey("should error if publisher status is cancelled", func() {
			pub.setStatus(statusCancelled)
			err := pub.Publish(testMsg)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring,
				fmt.Sprintf("Publish() can not be called on consumer in %q state", statusCancelled))
		})

		Convey("should error if publisher status is created", func() {
			pub.setStatus(statusCreated)
			err := pub.Publish(testMsg)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring,
				fmt.Sprintf("Publish() can not be called on consumer in %q state", statusCreated))
		})

		Convey("should error if publish fails", func() {
			fakeChan.PublishReturns(errors.New("kaboom"))
			err := pub.Publish(testMsg)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "kaboom")
		})
	})
}

func Test_publisher_PublishWithRetries(t *testing.T) {
	Convey("Publish", t, func() {
		active := statusActive
		fakeChan := &fakes.FakeAMQPChannel{}
		fakeMux := &fakes.FakeRwLocker{}

		opts := &PublishOptions{
			ExchangeName: "the_exchange",
			RoutingKey:   "route_me",
			Mandatory:    false,
			Immediate:    false,
		}

		pub := &publisher{
			id:       "fooID",
			status:   &active,
			opts:     opts,
			amqpChan: fakeChan,
			chanMux:  fakeMux,
		}

		testBody := "this is a test"
		testMsg := amqp.Publishing{
			ContentType: "test/plain",
			Body:        []byte(testBody),
		}

		backoff := []time.Duration{time.Millisecond}

		Convey("should publish only once if successful on first try", func() {
			err := pub.PublishWithRetries(testMsg, backoff)
			So(err, ShouldBeNil)

			// calls publish
			So(fakeChan.PublishCallCount(), ShouldEqual, 1)
			exchange, routingKey, mandatory, immediate, msg := fakeChan.PublishArgsForCall(0)
			So(exchange, ShouldEqual, opts.ExchangeName)
			So(routingKey, ShouldEqual, opts.RoutingKey)
			So(mandatory, ShouldEqual, opts.Mandatory)
			So(immediate, ShouldEqual, opts.Immediate)
			So(msg, ShouldResemble, testMsg)
		})

		Convey("should error if publisher status is not active", func() {
			pub.setStatus(statusCancelled)
			err := pub.PublishWithRetries(testMsg, backoff)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring,
				fmt.Sprintf("Publish() can not be called on consumer in %q state", statusCancelled))
		})

		Convey("should retry if publish fails", func() {
			fakeChan.PublishReturns(errors.New("kaboom"))

			err := pub.PublishWithRetries(testMsg, backoff)
			So(fakeChan.PublishCallCount(), ShouldEqual, 2)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "kaboom")
		})

		Convey("retries until it succeeds ", func() {
			fakeChan.PublishReturns(errors.New("kaboom"))
			// success on third try
			fakeChan.PublishReturnsOnCall(2, nil)
			// 3 retries, so total would be 4 tries
			backoff := []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond}

			err := pub.PublishWithRetries(testMsg, backoff)
			// should only get 3
			So(fakeChan.PublishCallCount(), ShouldEqual, 3)
			So(err, ShouldBeNil)
		})
	})
}

func Test_publisher_Close(t *testing.T) {
	Convey("Close", t, func() {
		active := statusActive
		fakeChan := &fakes.FakeAMQPChannel{}
		fakeMux := &fakes.FakeRwLocker{}

		opts := &PublishOptions{
			ExchangeName: "the_exchange",
			RoutingKey:   "route_me",
			Mandatory:    false,
			Immediate:    false,
		}

		pub := &publisher{
			id:       "fooID",
			status:   &active,
			opts:     opts,
			amqpChan: fakeChan,
			chanMux:  fakeMux,
		}

		Convey("closes the channel and removed itself from the parent", func() {
			rmCalled := ""
			pub.rmCallback = func(s string) {
				// record the value used to call the callback func
				rmCalled = s
			}

			err := pub.Close()
			So(err, ShouldBeNil)
			So(*pub.status, ShouldEqual, statusCancelled)
			So(rmCalled, ShouldEqual, pub.id)
		})

		Convey("errors if chan close fails", func() {
			fakeChan.CloseReturns(errors.New("boom"))

			err := pub.Close()
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "boom")
		})
	})
}

func Test_publisher_restart(t *testing.T) {
	Convey("restart", t, func() {
		active := statusActive
		origChan := &fakes.FakeAMQPChannel{}
		fakeMux := &fakes.FakeRwLocker{}

		opts := &PublishOptions{
			ExchangeName: "the_exchange",
			RoutingKey:   "route_me",
			Mandatory:    false,
			Immediate:    false,
		}

		pub := &publisher{
			id:       "fooID",
			status:   &active,
			opts:     opts,
			amqpChan: origChan,
			chanMux:  fakeMux,
		}

		newChan := &fakes.FakeAMQPChannel{}

		Convey("restarts the publisher", func() {
			setupCalled := false
			pub.chanSetupFuncs = []SetupFunc{func(ch *amqp.Channel) error { setupCalled = true; return nil }}

			err := pub.restart(newChan)
			So(err, ShouldBeNil)
			So(pub.amqpChan, ShouldEqual, newChan)
			So(pub.amqpChan, ShouldNotEqual, origChan)
			So(setupCalled, ShouldBeTrue)
		})

		Convey("executes all setup funcs", func() {
			var first, second, third bool
			pub.chanSetupFuncs = []SetupFunc{
				func(ch *amqp.Channel) error { first = true; return nil },
				func(ch *amqp.Channel) error { second = true; return nil },
				func(ch *amqp.Channel) error { third = true; return nil },
			}

			err := pub.restart(newChan)
			So(err, ShouldBeNil)
			So(first, ShouldBeTrue)
			So(second, ShouldBeTrue)
			So(third, ShouldBeTrue)
		})

		Convey("does not error if chan setup func is nil", func() {
			pub.chanSetupFuncs = nil

			err := pub.restart(newChan)
			So(err, ShouldBeNil)
		})

		Convey("errors if chan setup fails", func() {
			pub.chanSetupFuncs = []SetupFunc{func(ch *amqp.Channel) error { return errors.New("boom") }}

			err := pub.restart(newChan)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "failed to setup channel topology on publisher restart")
		})

		Convey("errors if one setup fails in the chain and further execution is halted", func() {
			var first, third bool
			pub.chanSetupFuncs = []SetupFunc{
				func(ch *amqp.Channel) error { first = true; return nil },
				func(ch *amqp.Channel) error { return errors.New("boom") },
				func(ch *amqp.Channel) error { third = true; return nil },
			}

			err := pub.restart(newChan)
			So(err, ShouldNotBeNil)
			So(first, ShouldBeTrue)
			So(third, ShouldBeFalse)
		})
	})
}

func Test_publisher_helpers(t *testing.T) {
	Convey("helpers", t, func() {
		origStatus := statusActive
		fakeChan := &fakes.FakeAMQPChannel{}
		fakeMux := &fakes.FakeRwLocker{}

		pub := &publisher{
			id:       "fooID",
			status:   &origStatus,
			opts:     &PublishOptions{},
			amqpChan: fakeChan,
			chanMux:  fakeMux,
		}

		Convey("setStatus", func() {
			Convey("sets status correctly when status is nil", func() {
				pub.status = nil
				pub.setStatus(statusCancelled)
				So(*pub.status, ShouldEqual, statusCancelled)
			})

			Convey("sets status correctly when status is set", func() {
				pub.setStatus(statusCancelled)
				So(*pub.status, ShouldEqual, statusCancelled)
			})
		})

		Convey("getStatus", func() {
			Convey("gets status", func() {
				stat := pub.getStatus()
				So(stat, ShouldEqual, origStatus)
			})

			Convey("status defaults to created when nil", func() {
				pub.status = nil
				So(origStatus, ShouldNotEqual, statusCreated)

				stat := pub.getStatus()
				So(stat, ShouldEqual, statusCreated)
			})
		})

		Convey("getID", func() {
			So(pub.getID(), ShouldEqual, pub.id)
		})
	})
}
