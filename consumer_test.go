package bunny

import (
	"errors"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/streadway/amqp"

	"github.com/Shimmur/bunny/fakes"
)

func Test_NewConsumerChannel(t *testing.T) {
	Convey("NewConsumerChannel", t, func() {
		bunn := NewRabbit(ConnectionDetails{
			URLs: []string{"foobar"},
		})

		fakeDialer := &fakes.FakeAMQPDialer{}
		fakeConn := &fakes.FakeAMQPConnection{}
		fakeDialer.DialReturns(fakeConn, nil)

		bunn.connDetails.dialer = fakeDialer

		err := bunn.Connect()
		So(err, ShouldBeNil)

		setup := func(ch *amqp.Channel) error { return nil }

		Convey("creates a new consumer channel and calls the setup func", func() {
			setupCalled := false
			setup := func(ch *amqp.Channel) error { setupCalled = true; return nil }

			c, err := bunn.NewConsumerChannel(setup)
			So(err, ShouldBeNil)
			So(setupCalled, ShouldBeTrue)
			So(c, ShouldNotBeNil)
			cons, ok := c.(*consumer)
			So(ok, ShouldBeTrue)
			So(cons.chanSetupFuncs[0], ShouldEqual, setup)
			So(cons.id, ShouldNotBeEmpty)
			So(cons.deliveryMux, ShouldNotBeNil)
			So(*cons.status, ShouldEqual, statusCreated)
		})

		Convey("errors if setup func fails", func() {
			setup := func(ch *amqp.Channel) error { return errors.New("kaboom") }

			c, err := bunn.NewConsumerChannel(setup)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "failed to setup channel with provided func")
			So(c, ShouldBeNil)
		})

		Convey("errors if there is no connection", func() {
			bunn.connections = nil

			_, err := bunn.NewConsumerChannel(setup)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "no connection! Must call Connect()")
		})
	})
}

func Test_consumer_Consume(t *testing.T) {
	Convey("Consume", t, func() {
		created := statusCreated
		fakeChan := &fakes.FakeAMQPChannel{}
		fakeMux := &fakes.FakeRwLocker{}

		cons := &consumer{
			id:          "fooID",
			status:      &created,
			amqpChan:    fakeChan,
			deliveryMux: fakeMux,
		}

		defaultConsumeFunc := func(msg *amqp.Delivery) error { return nil }
		defaultConsumeOpts := ConsumeOptions{}
		defaultErrChan := make(chan<- error)

		Convey("begins consuming", func() {
			err := cons.Consume(defaultConsumeFunc, defaultConsumeOpts, defaultErrChan)
			So(err, ShouldBeNil)
			So(cons.consumeFunc, ShouldEqual, defaultConsumeFunc)
			So(*cons.opts, ShouldResemble, defaultConsumeOpts)
			So(cons.errorChan, ShouldEqual, defaultErrChan)
		})

		Convey("qos settings", func() {
			consumeOpts := ConsumeOptions{
				QoSOptions: QoSOptions{
					PrefetchCount: 100,
					PrefetchSize:  1000,
					Global:        true,
				},
			}
			err := cons.Consume(defaultConsumeFunc, consumeOpts, defaultErrChan)
			So(err, ShouldBeNil)
			So(fakeChan.QosCallCount(), ShouldEqual, 1)
			pCount, pSize, pGlobe := fakeChan.QosArgsForCall(0)
			So(pCount, ShouldEqual, 100)
			So(pSize, ShouldEqual, 1000)
			So(pGlobe, ShouldEqual, true)
		})

		Convey("errors if consuming has already begun", func() {
			stat := statusActive
			cons.status = &stat

			err := cons.Consume(defaultConsumeFunc, defaultConsumeOpts, defaultErrChan)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, `Consume() can not be called on consumer in "active" state`)
		})

		Convey("errors if consuming fails", func() {
			fakeChan.ConsumeReturns(nil, errors.New("kaboom"))

			err := cons.Consume(defaultConsumeFunc, defaultConsumeOpts, defaultErrChan)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "failed to begin consuming from channel")
		})
	})
}

// this is tested seperately because it is called from both Consume() and restart()
func Test_consumer_consume(t *testing.T) {
	Convey("consume", t, func() {
		created := statusCreated
		fakeChan := &fakes.FakeAMQPChannel{}
		fakeMux := &fakes.FakeRwLocker{}
		testDeliveryChan := make(<-chan amqp.Delivery)
		fakeChan.ConsumeReturns(testDeliveryChan, nil)

		cons := &consumer{
			id:          "fooID",
			status:      &created,
			amqpChan:    fakeChan,
			deliveryMux: fakeMux,
			consumeFunc: func(msg *amqp.Delivery) error { return nil },
			opts:        &ConsumeOptions{},
			errorChan:   make(chan<- error),
		}

		Convey("begins consuming", func() {
			err := cons.consume()
			So(err, ShouldBeNil)
			So(*cons.status, ShouldEqual, statusActive)          // status was set
			So(cons.deliveryChan, ShouldEqual, testDeliveryChan) // delivery was saved
			So(fakeMux.LockCallCount(), ShouldEqual, 1)
			So(fakeMux.UnlockCallCount(), ShouldEqual, 1)
		})

		Convey("consumer opts are honored", func() {
			queueName := "foobar"
			cons.opts = &ConsumeOptions{
				QueueName: queueName,
				AutoAck:   true,
				Exclusive: true,
				NoWait:    true,
			}

			err := cons.consume()
			So(err, ShouldBeNil)
			qname, tag, autoAck, exclusive, noLocal, noWait, args := fakeChan.ConsumeArgsForCall(0)
			So(qname, ShouldEqual, queueName)
			So(tag, ShouldEqual, cons.consumerTag)
			So(autoAck, ShouldEqual, cons.opts.AutoAck)
			So(exclusive, ShouldEqual, cons.opts.Exclusive)
			So(noLocal, ShouldBeFalse) // always false
			So(noWait, ShouldEqual, cons.opts.NoWait)
			So(args, ShouldBeNil) // always nil for now
		})

		Convey("starts the consumer loop and receives deliveries", func() {
			testDeliveryChan := make(chan amqp.Delivery)
			fakeChan.ConsumeReturns(testDeliveryChan, nil)
			var gotMsg *amqp.Delivery
			cons.consumeFunc = func(msg *amqp.Delivery) error { gotMsg = msg; return nil }

			err := cons.consume()
			So(err, ShouldBeNil)

			testMsg := amqp.Delivery{CorrelationId: "this is the delivery"} // use this ID to assert equality
			testDeliveryChan <- testMsg
			time.Sleep(time.Millisecond * 5)
			So(gotMsg.CorrelationId, ShouldEqual, testMsg.CorrelationId)
		})

		Convey("errors if consuming fails", func() {
			fakeChan.ConsumeReturns(nil, errors.New("kaboom"))

			err := cons.consume()
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "failed to begin consuming from channel")
		})
	})
}

func Test_consumer_consumeLoop(t *testing.T) {
	Convey("consumeLoop", t, func() {
		fakeMux := &fakes.FakeRwLocker{}
		testDeliveryChan := make(chan amqp.Delivery)
		testErrChan := make(chan error)

		cons := &consumer{
			id:           "fooID",
			consumeFunc:  func(msg *amqp.Delivery) error { return nil },
			deliveryChan: testDeliveryChan,
			errorChan:    testErrChan,
			deliveryMux:  fakeMux,
		}

		Convey("runs the consumer func against deliveries", func() {
			var gotMsg *amqp.Delivery
			cons.consumeFunc = func(msg *amqp.Delivery) error { gotMsg = msg; return nil }

			go cons.consumeLoop()

			testMsg := amqp.Delivery{CorrelationId: "this is the delivery"} // use this ID to assert equality
			testDeliveryChan <- testMsg
			So(gotMsg.CorrelationId, ShouldEqual, testMsg.CorrelationId)
		})

		Convey("consumer loop exist if delivery channel is closed", func() {
			capture := captureLogs()

			done := false
			go func() {
				done = cons.consumeLoop()
			}()

			close(testDeliveryChan)

			// allow end to happen
			time.Sleep(time.Millisecond * 5)

			So(done, ShouldBeTrue)
			So(capture.String(), ShouldContainSubstring, "got delivery channel close!")
		})

		Convey("errors is sent on error chan if consumer func errors", func() {
			expErr := errors.New("kabloom")
			cons.consumeFunc = func(msg *amqp.Delivery) error { return expErr }

			go cons.consumeLoop()

			testMsg := amqp.Delivery{}
			testDeliveryChan <- testMsg

			err := <-testErrChan
			So(err, ShouldNotBeNil)
			So(err, ShouldEqual, expErr)
		})
	})
}

func Test_consumer_Cancel(t *testing.T) {
	Convey("Cancel", t, func() {
		consuming := statusActive
		fakeChan := &fakes.FakeAMQPChannel{}
		fakeMux := &fakes.FakeRwLocker{}
		testDeliveryChan := make(<-chan amqp.Delivery)
		fakeChan.ConsumeReturns(testDeliveryChan, nil)

		cons := &consumer{
			id:          "fooID",
			status:      &consuming,
			amqpChan:    fakeChan,
			deliveryMux: fakeMux,
			consumeFunc: func(msg *amqp.Delivery) error { return nil },
			opts:        &ConsumeOptions{},
			errorChan:   make(chan<- error),
			rmCallback:  func(s string) {},
		}

		Convey("cancels the consumer", func() {
			nowait := true

			err := cons.Cancel(nowait)
			So(err, ShouldBeNil)
			So(*cons.status, ShouldEqual, statusCancelled)
			So(fakeChan.CancelCallCount(), ShouldEqual, 1)
			So(fakeChan.CloseCallCount(), ShouldEqual, 1)
			tag, nw := fakeChan.CancelArgsForCall(0)
			So(nw, ShouldEqual, nowait)
			So(tag, ShouldEqual, cons.consumerTag)
		})

		Convey("removes itself from the consumer pool", func() {
			called := false
			cons.rmCallback = func(s string) { called = true }

			err := cons.Cancel(false)
			So(err, ShouldBeNil)
			So(called, ShouldBeTrue)
		})

		Convey("errors if can not be cancelled", func() {
			fakeChan.CancelReturns(errors.New("kabloooom"))

			err := cons.Cancel(false)
			So(err, ShouldNotBeNil)
		})

		Convey("does not error if channel is already closed", func() {
			called := false
			cons.rmCallback = func(s string) { called = true }
			fakeChan.CancelReturns(amqp.ErrClosed)

			err := cons.Cancel(false)
			So(err, ShouldBeNil)
			// still sets status to cancelled and removes from pool
			So(*cons.status, ShouldEqual, statusCancelled)
			So(called, ShouldBeTrue)
			// does not call close on already closed channel
			So(fakeChan.CloseCallCount(), ShouldEqual, 0)
		})

		Convey("errors if can not be closed", func() {
			fakeChan.CloseReturns(errors.New("boom boom"))

			err := cons.Cancel(false)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "failed to close channel for consumer")
		})
	})
}

func Test_consumer_restart(t *testing.T) {
	Convey("restart", t, func() {
		consuming := statusActive
		origChan := &fakes.FakeAMQPChannel{}
		fakeMux := &fakes.FakeRwLocker{}
		testDeliveryChan := make(<-chan amqp.Delivery)
		origChan.ConsumeReturns(testDeliveryChan, nil)

		cons := &consumer{
			id:             "fooID",
			status:         &consuming,
			amqpChan:       origChan,
			deliveryMux:    fakeMux,
			consumeFunc:    func(msg *amqp.Delivery) error { return nil },
			opts:           &ConsumeOptions{},
			errorChan:      make(chan<- error),
			chanSetupFuncs: []SetupFunc{func(ch *amqp.Channel) error { return nil }},
		}

		newChan := &fakes.FakeAMQPChannel{}

		Convey("restarts the consumer", func() {
			setupCalled := false
			cons.chanSetupFuncs = []SetupFunc{func(ch *amqp.Channel) error { setupCalled = true; return nil }}

			err := cons.restart(newChan)
			So(err, ShouldBeNil)
			So(cons.amqpChan, ShouldEqual, newChan)
			So(setupCalled, ShouldBeTrue)
			So(newChan.ConsumeCallCount(), ShouldEqual, 1)
			So(origChan.ConsumeCallCount(), ShouldEqual, 0)
		})

		Convey("executes all setup funcs", func() {
			var first, second, third bool
			cons.chanSetupFuncs = []SetupFunc{
				func(ch *amqp.Channel) error { first = true; return nil },
				func(ch *amqp.Channel) error { second = true; return nil },
				func(ch *amqp.Channel) error { third = true; return nil },
			}

			err := cons.restart(newChan)
			So(err, ShouldBeNil)
			So(first, ShouldBeTrue)
			So(second, ShouldBeTrue)
			So(third, ShouldBeTrue)
		})

		Convey("does not error if chan setup func is nil", func() {
			cons.chanSetupFuncs = nil

			err := cons.restart(newChan)
			So(err, ShouldBeNil)
		})

		Convey("errors if chan setup fails", func() {
			cons.chanSetupFuncs = []SetupFunc{func(ch *amqp.Channel) error { return errors.New("boom") }}

			err := cons.restart(newChan)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "failed to setup channel topology on restart")
		})

		Convey("errors if one setup fails in the chain and further execution is halted", func() {
			var first, third bool
			cons.chanSetupFuncs = []SetupFunc{
				func(ch *amqp.Channel) error { first = true; return nil },
				func(ch *amqp.Channel) error { return errors.New("boom") },
				func(ch *amqp.Channel) error { third = true; return nil },
			}

			err := cons.restart(newChan)
			So(err, ShouldNotBeNil)
			So(first, ShouldBeTrue)
			So(third, ShouldBeFalse)
		})

		Convey("errors if consume fails", func() {
			newChan.ConsumeReturns(nil, errors.New("boom"))

			err := cons.restart(newChan)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "failed to begin consuming from channel on consumer restart")
		})
	})
}

func Test_consumer_helpers(t *testing.T) {
	Convey("helpers", t, func() {
		origStatus := statusActive

		cons := &consumer{
			id:     "fooID",
			status: &origStatus,
		}

		Convey("setStatus", func() {
			Convey("sets status correctly when status is nil", func() {
				cons.status = nil
				cons.setStatus(statusCancelled)
				So(*cons.status, ShouldEqual, statusCancelled)
			})

			Convey("sets status correctly when status is set", func() {
				cons.setStatus(statusCancelled)
				So(*cons.status, ShouldEqual, statusCancelled)
			})
		})

		Convey("getStatus", func() {
			Convey("gets status", func() {
				stat := cons.getStatus()
				So(stat, ShouldEqual, origStatus)
			})

			Convey("status defaults to created when nil", func() {
				cons.status = nil
				So(origStatus, ShouldNotEqual, statusCreated)

				stat := cons.getStatus()
				So(stat, ShouldEqual, statusCreated)
			})
		})

		Convey("getID", func() {
			So(cons.getID(), ShouldEqual, cons.id)
		})
	})
}
