package bunny

import (
	"bytes"
	"errors"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/streadway/amqp"

	"github.com/Shimmur/bunny/fakes"
)

/*-----------------------
| Connection Pool Tests |
-----------------------*/

func Test_newPool(t *testing.T) {
	Convey("newPool", t, func() {
		fakeDialer := &fakes.FakeAMQPDialer{}
		fakeConn := &fakes.FakeAMQPConnection{}
		fakeDialer.DialReturns(fakeConn, nil)

		cd := &ConnectionDetails{
			URLs:                     []string{"foobar"},
			dialer:                   fakeDialer,
			maxChannelsPerConnection: maxChannelsPerConnection,
		}

		Convey("creates a new connection and returns the pool", func() {
			before := runtime.NumGoroutine()
			pool, err := newPool(cd)
			after := runtime.NumGoroutine()

			So(err, ShouldBeNil)
			So(len(pool.conns), ShouldEqual, 1)
			So(pool.connPoolMux, ShouldNotBeNil)
			So(pool.details, ShouldEqual, cd)
			// Rebalancing should have kicked off
			//  not the best way to test this but will do for now
			// TODO: maybe capture logs and test for debug logging
			//  would also need to make the interval configurable
			So(after, ShouldBeGreaterThan, before)
		})

		Convey("errors if new connection fails", func() {
			cd.URLs = nil
			_, err := newPool(cd)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "no AMQP URLs were supplied")
		})
	})
}

func Test_connPool_declareInitialTopology(t *testing.T) {
	Convey("declareInitialTopology", t, func() {
		fakeDialer := &fakes.FakeAMQPDialer{}
		fakeConn := &fakes.FakeAMQPConnection{}
		fakeDialer.DialReturns(fakeConn, nil)

		cd := &ConnectionDetails{
			URLs:                     []string{"foobar"},
			dialer:                   fakeDialer,
			maxChannelsPerConnection: maxChannelsPerConnection,
		}

		pool, err := newPool(cd)
		So(err, ShouldBeNil)

		var conn *connection
		// just grab the first one
		for _, conn = range pool.conns {
			break
		}

		// mock the connection
		fakeConnection := &fakes.FakeAMQPConnection{}
		fakeChannel := &fakes.FakeAMQPChannel{}
		fakeConnection.ChannelReturns(fakeChannel, nil)
		conn.amqpConn = fakeConnection

		Convey("gets next available connection and declares the topology", func() {
			called := false
			setup := func(*amqp.Channel) error { called = true; return nil }

			err := pool.declareInitialTopology(setup)
			So(err, ShouldBeNil)
			So(called, ShouldBeTrue)
		})

		Convey("errors if called twice", func() {
			err := pool.declareInitialTopology(func(*amqp.Channel) error { return nil })
			So(err, ShouldBeNil)

			err = pool.declareInitialTopology(func(*amqp.Channel) error { return nil })
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "more than once")
		})

		Convey("errors if connection can not be obtained", func() {
			pool.conns = map[string]*connection{}
			pool.details = nil
			err := pool.declareInitialTopology(func(*amqp.Channel) error { return nil })
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "failed to obtain connection")
		})

		Convey("errors if fail to declare topology", func() {
			err := pool.declareInitialTopology(func(*amqp.Channel) error { return errors.New("kaboom") })
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "kaboom")
		})
	})
}

func Test_connPool_newConnection(t *testing.T) {
	Convey("newConnection", t, func() {
		fakeDialer := &fakes.FakeAMQPDialer{}
		fakeConn := &fakes.FakeAMQPConnection{}
		fakeDialer.DialReturns(fakeConn, nil)

		cd := &ConnectionDetails{
			URLs:                     []string{"foobar"},
			dialer:                   fakeDialer,
			maxChannelsPerConnection: maxChannelsPerConnection,
		}

		pool, err := newPool(cd)
		So(err, ShouldBeNil)

		Convey("new connection is created and returned", func() {
			conn, err := pool.newConnection()
			So(err, ShouldBeNil)
			So(conn, ShouldNotBeNil)
			So(conn.details, ShouldEqual, pool.details)
			So(conn.topologyDef, ShouldEqual, pool.topologyDef)
			So(conn.connMux, ShouldNotBeNil)
			So(conn.consumers, ShouldNotBeNil)
			So(conn.consumerMux, ShouldNotBeNil)
			So(conn.rmCallback, ShouldEqual, pool.deleteConnection)

			// appends to pool
			So(pool.conns, ShouldContainKey, conn.id)
			So(pool.conns[conn.id], ShouldEqual, conn)
			So(pool.currentID, ShouldEqual, conn.id)
		})

		Convey("errors connect fails", func() {
			fakeDialer.DialReturns(nil, errors.New("failed dial"))

			_, err := pool.newConnection()
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "failed dial")
		})
	})
}

func Test_connPool_getNext(t *testing.T) {
	Convey("getNext()", t, func() {
		fakeDialer := &fakes.FakeAMQPDialer{}
		fakeConn := &fakes.FakeAMQPConnection{}
		fakeDialer.DialReturns(fakeConn, nil)

		cd := &ConnectionDetails{
			URLs:                     []string{"foobar"},
			dialer:                   fakeDialer,
			maxChannelsPerConnection: maxChannelsPerConnection,
		}

		pool, err := newPool(cd)
		So(err, ShouldBeNil)

		var connID string
		// just grab the first one
		for connID, _ = range pool.conns {
			break
		}

		Convey("returns the connection if there is only one", func() {
			conn, err := pool.getNext()
			So(err, ShouldBeNil)
			So(conn.id, ShouldEqual, connID)
		})

		Convey("returns the latest one if there is more than one", func() {
			// add a new connection
			newConn, err := pool.newConnection()
			So(err, ShouldBeNil)

			conn, err := pool.getNext()
			So(err, ShouldBeNil)
			So(conn.id, ShouldEqual, newConn.id)
		})

		Convey("creates a new connection if there are none", func() {
			// wipe it out
			pool.conns = map[string]*connection{}

			_, err := pool.getNext()
			So(err, ShouldBeNil)
			So(len(pool.conns), ShouldEqual, 1)
		})

		Convey("creates a new connection if conns is nil", func() {
			pool.conns = nil

			_, err := pool.getNext()
			So(err, ShouldBeNil)
			So(len(pool.conns), ShouldEqual, 1)
		})

		Convey("locks the mutex", func() {
			fakeMux := &fakes.FakeLocker{}
			pool.connPoolMux = fakeMux

			conn, err := pool.getNext()
			So(err, ShouldBeNil)
			So(conn, ShouldNotBeNil)
			So(fakeMux.LockCallCount(), ShouldEqual, 1)
			So(fakeMux.UnlockCallCount(), ShouldEqual, 1)
		})

		Convey("creates a new connection if consumer is at max capacity", func() {
			pool.details.maxChannelsPerConnection = 1
			// put a dummy consumer in there
			pool.conns[connID].consumers["foo"] = &consumer{}

			conn, err := pool.getNext()
			So(err, ShouldBeNil)
			So(conn, ShouldNotEqual, pool.conns[connID])
			So(len(pool.conns), ShouldEqual, 2)
		})

		Convey("errors if new connection fails", func() {
			pool.details.maxChannelsPerConnection = 1
			// put a dummy consumer in there
			pool.conns[connID].consumers["foo"] = &consumer{}
			fakeDialer.DialReturns(nil, errors.New("failed connection"))

			_, err := pool.getNext()
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "failed connection")
		})
	})
}

func Test_connPool_establishConsumerChan(t *testing.T) {
	Convey("establishConsumerChan", t, func() {
		fakeDialer := &fakes.FakeAMQPDialer{}
		fakeConn := &fakes.FakeAMQPConnection{}
		fakeDialer.DialReturns(fakeConn, nil)

		cd := &ConnectionDetails{
			URLs:                     []string{"foobar"},
			dialer:                   fakeDialer,
			maxChannelsPerConnection: maxChannelsPerConnection,
		}

		pool, err := newPool(cd)
		So(err, ShouldBeNil)

		var conn *connection

		// just grab the first one
		for _, conn = range pool.conns {
			break
		}

		// mock the connection
		fakeConnection := &fakes.FakeAMQPConnection{}
		conn.amqpConn = fakeConnection
		// mock the locker
		fakeRWMux := &fakes.FakeRwLocker{}
		conn.consumerMux = fakeRWMux

		consumerID := "foo"
		consumer := &consumer{
			id:             consumerID,
			chanSetupFuncs: []SetupFunc{func(*amqp.Channel) error { return nil }},
			consumeFunc:    nil,
			opts:           nil,
			errorChan:      nil,
			deliveryMux:    &sync.RWMutex{},
		}

		Convey("establishes a new channel for consuming", func() {
			err := pool.establishConsumerChan(consumer)
			So(err, ShouldBeNil)

			// consumer was registered
			So(conn.consumers, ShouldContainKey, consumerID)
			So(conn.consumers[consumerID], ShouldEqual, consumer)
			So(fakeRWMux.LockCallCount(), ShouldEqual, 1)
			So(fakeRWMux.UnlockCallCount(), ShouldEqual, 1)
		})

		Convey("all user provided setup funcs are called", func() {
			var first, second, third bool
			consumer.chanSetupFuncs = []SetupFunc{
				func(ch *amqp.Channel) error { first = true; return nil },
				func(ch *amqp.Channel) error { second = true; return nil },
				func(ch *amqp.Channel) error { third = true; return nil },
			}

			err := pool.establishConsumerChan(consumer)
			So(err, ShouldBeNil)
			So(first, ShouldBeTrue)
			So(second, ShouldBeTrue)
			So(third, ShouldBeTrue)
		})

		Convey("does not error event if setup func is nil", func() {
			consumer.chanSetupFuncs = nil

			err := pool.establishConsumerChan(consumer)
			So(err, ShouldBeNil)
		})

		Convey("errors if get next fails", func() {
			// nil details causes get next to fail
			pool.details = nil

			err := pool.establishConsumerChan(consumer)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "cannot establish consumer channel")
		})

		Convey("errors if channel creation fails", func() {
			fakeConnection.ChannelReturns(nil, errors.New("no channel"))

			err := pool.establishConsumerChan(consumer)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "no channel")
		})

		Convey("errors if channel setup func fails", func() {
			consumer.chanSetupFuncs = []SetupFunc{func(*amqp.Channel) error { return errors.New("boom") }}

			err := pool.establishConsumerChan(consumer)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "boom")
		})
	})
}

func Test_connPool_establishPublisherChan(t *testing.T) {
	Convey("establishPublisherChan()", t, func() {
		fakeDialer := &fakes.FakeAMQPDialer{}
		fakeConn := &fakes.FakeAMQPConnection{}
		fakeDialer.DialReturns(fakeConn, nil)

		cd := &ConnectionDetails{
			URLs:                     []string{"foobar"},
			dialer:                   fakeDialer,
			maxChannelsPerConnection: maxChannelsPerConnection,
		}

		pool, err := newPool(cd)
		So(err, ShouldBeNil)

		var conn *connection

		// just grab the first one
		for _, conn = range pool.conns {
			break
		}

		// mock the connection
		fakeConnection := &fakes.FakeAMQPConnection{}
		conn.amqpConn = fakeConnection
		// mock the locker
		fakeRWMux := &fakes.FakeRwLocker{}
		conn.publisherMux = fakeRWMux

		publisherID := "foo"
		pub := &publisher{
			id:             publisherID,
			chanSetupFuncs: []SetupFunc{func(*amqp.Channel) error { return nil }},
			opts:           nil,
			chanMux:        &sync.RWMutex{},
		}

		Convey("establishes a new channel for publishing", func() {
			err := pool.establishPublisherChan(pub)
			So(err, ShouldBeNil)

			// publisher was registered
			So(conn.publishers, ShouldContainKey, publisherID)
			So(conn.publishers[publisherID], ShouldEqual, pub)
			So(fakeRWMux.LockCallCount(), ShouldEqual, 1)
			So(fakeRWMux.UnlockCallCount(), ShouldEqual, 1)
		})

		Convey("all user provided setup funcs are called", func() {
			var first, second, third bool
			pub.chanSetupFuncs = []SetupFunc{
				func(ch *amqp.Channel) error { first = true; return nil },
				func(ch *amqp.Channel) error { second = true; return nil },
				func(ch *amqp.Channel) error { third = true; return nil },
			}

			err := pool.establishPublisherChan(pub)
			So(err, ShouldBeNil)
			So(first, ShouldBeTrue)
			So(second, ShouldBeTrue)
			So(third, ShouldBeTrue)
		})

		Convey("does not error event if setup func is nil", func() {
			pub.chanSetupFuncs = nil

			err := pool.establishPublisherChan(pub)
			So(err, ShouldBeNil)
		})

		Convey("errors if get next fails", func() {
			// nil details causes get next to fail
			pool.details = nil

			err := pool.establishPublisherChan(pub)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "cannot establish publisher channel")
		})

		Convey("errors if channel creation fails", func() {
			fakeConnection.ChannelReturns(nil, errors.New("no channel"))

			err := pool.establishPublisherChan(pub)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "no channel")
		})

		Convey("errors if channel setup func fails", func() {
			pub.chanSetupFuncs = []SetupFunc{func(*amqp.Channel) error { return errors.New("boom") }}

			err := pool.establishPublisherChan(pub)
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "boom")
		})
	})
}

func Test_connPool_rebalance(t *testing.T) {
	Convey("rebalance", t, func() {
		// TODO: implement
	})
}

func Test_connPool_deleteConnection(t *testing.T) {
	Convey("deleteConnection", t, func() {
		fakeMux := &fakes.FakeLocker{}
		pool := &connPool{
			connPoolMux: fakeMux,
			conns:       map[string]*connection{},
		}

		Convey("deletes the connection and locks the mutex", func() {
			id := "foo"
			pool.conns = map[string]*connection{id: {}, "bar": {}} // need two to test deletion

			ok := pool.deleteConnection(id)
			So(ok, ShouldBeTrue)
			So(len(pool.conns), ShouldEqual, 1) // one remaining
			So(pool.conns, ShouldNotBeNil)
			So(fakeMux.LockCallCount(), ShouldEqual, 1)
			So(fakeMux.UnlockCallCount(), ShouldEqual, 1)
		})

		Convey("if not present, do nothing", func() {
			ok := pool.deleteConnection("notarealID")
			So(ok, ShouldBeTrue)
			So(len(pool.conns), ShouldEqual, 0)
		})

		Convey("if there is only one remaining, does not delete", func() {
			id := "foo"
			pool.conns = map[string]*connection{id: {}} // only one

			ok := pool.deleteConnection(id)
			So(ok, ShouldBeFalse)
			So(pool.conns, ShouldContainKey, id)
		})
	})
}

func Test_connPool_reconnect(t *testing.T) {
	Convey("reconnects correctly", t, func() {
		fakeDialer := &fakes.FakeAMQPDialer{}
		fakeConn := &fakes.FakeAMQPConnection{}
		fakeDialer.DialReturns(fakeConn, nil)

		var closeChan chan *amqp.Error
		fakeConn.NotifyCloseStub = func(c chan *amqp.Error) chan *amqp.Error {
			closeChan = c
			return c
		}

		cd := &ConnectionDetails{
			URLs:                     []string{"foobar"},
			dialer:                   fakeDialer,
			maxChannelsPerConnection: maxChannelsPerConnection,
		}

		pool, err := newPool(cd)
		So(err, ShouldBeNil)

		Convey("if there is only one connection left on a pool, it survives a restart even if it has no consumers", func() {
			// verify there is only one
			So(len(pool.conns), ShouldEqual, 1)
			for _, conn := range pool.conns {
				// wipe out any consumers if there are any
				conn.consumers = map[string]restartable{}
			}

			// simulate connection close
			closeChan <- &amqp.Error{}
			// wait for reconnect to happen
			time.Sleep(time.Millisecond * 10)

			So(len(pool.conns), ShouldEqual, 1)
		})

		Convey("if a connection restarts and has no consumers, it will be deleted if there is another connection in the pool", func() {
			// verify there is only one
			So(len(pool.conns), ShouldEqual, 1)
			for _, conn := range pool.conns {
				// wipe out any consumers if there are any
				conn.consumers = map[string]restartable{}
			}
			// add another one
			pool.conns["other"] = &connection{}

			// simulate connection close
			closeChan <- &amqp.Error{}
			// wait for reconnect to happen
			time.Sleep(time.Millisecond * 10)

			So(len(pool.conns), ShouldEqual, 1)       // one was deleted
			So(pool.conns, ShouldContainKey, "other") // other still remains
		})
	})
}

/*------------------
| Connection Tests |
------------------*/

func Test_connection_connect(t *testing.T) {
	Convey("connect", t, func() {
		fakeDialer := &fakes.FakeAMQPDialer{}
		fakeConn := &fakes.FakeAMQPConnection{}
		fakeDialer.DialReturns(fakeConn, nil)
		fakeConn.NotifyCloseReturns(make(chan *amqp.Error))

		connDetails := &ConnectionDetails{
			URLs:                     []string{"foobar"},
			dialer:                   fakeDialer,
			maxChannelsPerConnection: maxChannelsPerConnection,
		}

		conn := &connection{
			details: connDetails,
		}

		Convey("dials and establishes a connection", func() {
			err := conn.connect()
			So(err, ShouldBeNil)
			So(fakeDialer.DialCallCount(), ShouldEqual, 1)
			So(conn.amqpConn, ShouldNotBeNil)
		})

		Convey("close notifications are watched", func() {
			before := runtime.NumGoroutine()
			err := conn.connect()
			after := runtime.NumGoroutine()
			So(err, ShouldBeNil)
			So(fakeDialer.DialCallCount(), ShouldEqual, 1)
			So(conn.notifyClose, ShouldNotBeNil)
			// watcher goroutine should be running
			// TODO: check logs or something to see that watcher is running
			So(after, ShouldBeGreaterThan, before)
		})

		Convey("if first URL fails, next one is attempted", func() {
			fakeDialer.DialReturnsOnCall(0, nil, errors.New("failed dial"))
			fakeConn := &fakes.FakeAMQPConnection{}
			fakeDialer.DialReturns(fakeConn, nil)
			connDetails.URLs = []string{"foo", "bar"}

			err := conn.connect()
			So(err, ShouldBeNil)
			So(fakeDialer.DialCallCount(), ShouldEqual, 2)
			So(conn.amqpConn, ShouldNotBeNil)
			So(conn.amqpConn, ShouldEqual, fakeConn)
		})

		Convey("uses TLS if specified", func() {
			fakeConn := &fakes.FakeAMQPConnection{}
			fakeDialer.DialReturns(fakeConn, nil)
			connDetails.UseTLS = true

			err := conn.connect()
			So(err, ShouldBeNil)
			So(fakeDialer.DialCallCount(), ShouldEqual, 1)
			_, cfg := fakeDialer.DialArgsForCall(0)
			So(cfg, ShouldNotBeNil) // used a TLS config
		})

		Convey("skips TLS verify if specified", func() {
			fakeConn := &fakes.FakeAMQPConnection{}
			fakeDialer.DialReturns(fakeConn, nil)
			connDetails.UseTLS = true
			connDetails.SkipVerifyTLS = true

			err := conn.connect()
			So(err, ShouldBeNil)
			So(fakeDialer.DialCallCount(), ShouldEqual, 1)
			_, cfg := fakeDialer.DialArgsForCall(0)
			So(cfg, ShouldNotBeNil)
			So(cfg.InsecureSkipVerify, ShouldBeTrue)
		})

		Convey("errors if details are missing", func() {
			conn.details = nil

			err := conn.connect()
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "no connection details found")
		})

		Convey("errors if no URLs given", func() {
			connDetails.URLs = nil

			err := conn.connect()
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "no AMQP URLs were supplied")
		})

		Convey("errors if all URLs fail", func() {
			fakeDialer.DialReturns(nil, errors.New("failed dial"))

			err := conn.connect()
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "failed dial")
		})
	})
}

func Test_connection_declareTopology(t *testing.T) {
	Convey("declareTopology", t, func() {
		fakeConnection := &fakes.FakeAMQPConnection{}
		fakeChannel := &fakes.FakeAMQPChannel{}
		fakeConnection.ChannelReturns(fakeChannel, nil)

		called := false

		conn := &connection{
			amqpConn:    fakeConnection,
			topologyDef: func(*amqp.Channel) error { called = true; return nil },
		}

		Convey("declares the topology", func() {
			err := conn.declareTopology()
			So(err, ShouldBeNil)
			So(called, ShouldBeTrue)
		})

		Convey("errors if fails to obtain channel", func() {
			fakeConnection.ChannelReturns(nil, errors.New("no channel"))
			err := conn.declareTopology()
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "no channel")
		})

		Convey("errors if fails to declare topology", func() {
			conn.topologyDef = func(*amqp.Channel) error { return errors.New("kaboom") }
			err := conn.declareTopology()
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "kaboom")
		})

		Convey("closes the channel when done", func() {
			// can not test this currently
		})
	})
}

func Test_connection_helpers(t *testing.T) {
	Convey("helpers", t, func() {
		fakeConsumerLocker := &fakes.FakeRwLocker{}
		fakePublisherLocker := &fakes.FakeRwLocker{}
		conn := &connection{
			consumerMux:  fakeConsumerLocker,
			publisherMux: fakePublisherLocker,
		}

		Convey("numChannels returns the number of consumers and publishers and locks mutex", func() {
			conn.consumers = map[string]restartable{"foo": &consumer{}, "bar": &consumer{}}
			conn.publishers = map[string]restartable{"foo": &publisher{}, "bar": &publisher{}}

			n := conn.numChannels()
			So(n, ShouldEqual, 4)
			So(fakeConsumerLocker.RLockCallCount(), ShouldEqual, 1)
			So(fakeConsumerLocker.RUnlockCallCount(), ShouldEqual, 1)
			So(fakePublisherLocker.RLockCallCount(), ShouldEqual, 1)
			So(fakePublisherLocker.RUnlockCallCount(), ShouldEqual, 1)
		})

		Convey("registerConsumers appends the consumer and locks mutex", func() {
			conn.consumers = map[string]restartable{}
			consumerID := "foobar"
			cons := &consumer{id: consumerID}

			conn.registerConsumer(cons)
			So(len(conn.consumers), ShouldEqual, 1)
			So(fakeConsumerLocker.LockCallCount(), ShouldEqual, 1)
			So(fakeConsumerLocker.UnlockCallCount(), ShouldEqual, 1)
			So(conn.consumers, ShouldContainKey, consumerID)
			So(conn.consumers[consumerID], ShouldEqual, cons)
		})

		Convey("registerPublisher appends the consumer and locks mutex", func() {
			conn.publishers = map[string]restartable{}
			publisherID := "foobar"
			cons := &publisher{id: publisherID}

			conn.registerPublisher(cons)
			So(len(conn.publishers), ShouldEqual, 1)
			So(fakePublisherLocker.LockCallCount(), ShouldEqual, 1)
			So(fakePublisherLocker.UnlockCallCount(), ShouldEqual, 1)
			So(conn.publishers, ShouldContainKey, publisherID)
			So(conn.publishers[publisherID], ShouldEqual, cons)
		})

		Convey("deleteConsumer deletes and locks mutex", func() {
			consumerID := "foo"
			conn.consumers = map[string]restartable{consumerID: &consumer{}, "bar": &consumer{}}

			conn.deleteConsumer(consumerID)
			So(len(conn.consumers), ShouldEqual, 1)
			So(fakeConsumerLocker.LockCallCount(), ShouldEqual, 1)
			So(fakeConsumerLocker.UnlockCallCount(), ShouldEqual, 1)
			So(conn.consumers, ShouldNotContainKey, consumerID)
		})

		Convey("deletePublisher deletes and locks mutex", func() {
			publisherID := "foo"
			conn.publishers = map[string]restartable{publisherID: &publisher{}, "bar": &publisher{}}

			conn.deletePublisher(publisherID)
			So(len(conn.publishers), ShouldEqual, 1)
			So(fakePublisherLocker.LockCallCount(), ShouldEqual, 1)
			So(fakePublisherLocker.UnlockCallCount(), ShouldEqual, 1)
			So(conn.publishers, ShouldNotContainKey, publisherID)
		})
	})
}

/*----------
| Restarts |
----------*/

func Test_connection_watchNotifyClose(t *testing.T) {
	Convey("watchNotifyClose", t, func() {
		fakeDialer := &fakes.FakeAMQPDialer{}
		fakeConn := &fakes.FakeAMQPConnection{}
		fakeDialer.DialReturns(fakeConn, nil)

		connDetails := &ConnectionDetails{
			URLs:                     []string{"foobar"},
			dialer:                   fakeDialer,
			maxChannelsPerConnection: maxChannelsPerConnection,
		}

		closeChan := make(chan *amqp.Error)
		fakeRWMuxC := &fakes.FakeRwLocker{}
		fakeRWMuxP := &fakes.FakeRwLocker{}
		conn := &connection{
			details:      connDetails,
			notifyClose:  closeChan,
			consumerMux:  fakeRWMuxC,
			publisherMux: fakeRWMuxP,
			rmCallback:   func(string) bool { return true },
		}

		Convey("reacts to a message on notifyClose channel", func() {
			// because this connection has no consumers, it will not restart itself
			// this behavior is desirable for this test as it allows us to confirm
			//  that restart was called, but we do not have to go through (and mock)
			//  the restart process
			conn.consumers = map[string]restartable{}

			// this is used as a way to see that the callback was called
			rmCalled := false
			conn.rmCallback = func(string) bool { rmCalled = true; return true }

			finished := false
			go func() {
				conn.watchNotifyClose()
				finished = true // signifies that watchNotifyClose returned
			}()

			closeChan <- &amqp.Error{}

			time.Sleep(time.Millisecond * 5)
			So(rmCalled, ShouldBeTrue) // restart was called
			So(finished, ShouldBeTrue) // watchNotifyClose returned
		})

		Convey("restarts if notifyClose channel is closed before anything is sent", func() {
			finished := false
			go func() {
				conn.watchNotifyClose()
				finished = true // signifies that watchNotifyClose returned
			}()

			close(closeChan)

			time.Sleep(time.Millisecond * 5)
			So(finished, ShouldBeTrue)
		})
	})
}

func Test_connection_restart(t *testing.T) {
	Convey("restart", t, func() {
		fakeDialer := &fakes.FakeAMQPDialer{}
		fakeConnection := &fakes.FakeAMQPConnection{}
		fakeDialer.DialReturns(fakeConnection, nil)
		fakeChannel := &fakes.FakeAMQPChannel{}
		fakeConnection.ChannelReturns(fakeChannel, nil)

		connDetails := &ConnectionDetails{
			URLs:                     []string{"foobar"},
			dialer:                   fakeDialer,
			maxChannelsPerConnection: maxChannelsPerConnection,
		}

		closeChan := make(chan *amqp.Error)
		fakeConsumerMux := &fakes.FakeRwLocker{}
		fakePublisherMux := &fakes.FakeRwLocker{}
		fakeMux := &fakes.FakeLocker{}
		fakeConsumer := &fakeRestartableConsumer{}
		fakePublisher := &fakeRestartableConsumer{}
		conn := &connection{
			details:      connDetails,
			notifyClose:  closeChan,
			connMux:      fakeMux,
			consumers:    map[string]restartable{"foo": fakeConsumer},
			publishers:   map[string]restartable{"foo": fakePublisher},
			consumerMux:  fakeConsumerMux,
			publisherMux: fakePublisherMux,
			rmCallback:   func(string) bool { return true },
		}

		Convey("restarts the connection", func() {
			capture := captureLogs()

			conn.restart()
			So(fakeDialer.DialCallCount(), ShouldEqual, 1)
			So(capture.String(), ShouldContainSubstring, "Successfully reconnected after")
		})

		Convey("if there are no consumers and publishers, it does not restart", func() {
			conn.consumers = map[string]restartable{}
			conn.publishers = map[string]restartable{}
			rmCalled := false
			conn.rmCallback = func(string) bool { rmCalled = true; return true }

			conn.restart()
			So(fakeDialer.DialCallCount(), ShouldEqual, 0)
			// removes itself from the pool
			So(rmCalled, ShouldBeTrue)
		})

		Convey("if it can not be deleted from the pool, it restarts, even if there are no consumers", func() {
			conn.consumers = map[string]restartable{}
			conn.rmCallback = func(string) bool { return false }

			conn.restart()
			So(fakeDialer.DialCallCount(), ShouldEqual, 1)
		})

		Convey("redeclares topology if defined", func() {
			fakeConnection := &fakes.FakeAMQPConnection{}
			fakeChannel := &fakes.FakeAMQPChannel{}
			fakeConnection.ChannelReturns(fakeChannel, nil)
			fakeDialer.DialReturns(fakeConnection, nil)
			conn.amqpConn = fakeConnection
			topologyCalled := false
			conn.topologyDef = func(ch *amqp.Channel) error { topologyCalled = true; return nil }

			conn.restart()
			So(topologyCalled, ShouldBeTrue)
		})

		Convey("restarts the consumers and publishers", func() {
			fakeConsumer.RestartReturns(nil)
			fakePublisher.RestartReturns(nil)

			conn.restart()

			So(fakeConsumerMux.LockCallCount(), ShouldEqual, 1)
			So(fakeConsumerMux.UnlockCallCount(), ShouldEqual, 1)
			So(fakeConsumer.RestartCallCount(), ShouldEqual, 1)
			So(fakePublisherMux.LockCallCount(), ShouldEqual, 1)
			So(fakePublisherMux.UnlockCallCount(), ShouldEqual, 1)
			So(fakePublisher.RestartCallCount(), ShouldEqual, 1)
		})

		Convey("restarts the publishers even if there are no consumers", func() {
			fakePublisher.RestartReturns(nil)

			conn.restart()

			So(fakePublisher.RestartCallCount(), ShouldEqual, 1)
		})

		Convey("restarts the consumers even if there are no publishers", func() {
			fakeConsumer.RestartReturns(nil)
			conn.publishers = map[string]restartable{}

			conn.restart()
			So(fakeConsumer.RestartCallCount(), ShouldEqual, 1)
		})

		Convey("logs an error if topology declaration fails", func() {
			capture := captureLogs()
			conn.topologyDef = func(ch *amqp.Channel) error { return nil }
			fakeConnection.ChannelReturns(nil, errors.New("boom"))

			conn.restart()
			So(capture.String(), ShouldContainSubstring, "could not declare topology on restart")
		})

		Convey("logs an error if restart consumer fails", func() {
			capture := captureLogs()

			fakeConsumer.RestartReturns(errors.New("boom"))

			conn.restart()
			So(capture.String(), ShouldContainSubstring, "Failed to restart consumers!")
		})
	})
}

func Test_connection_reconnect(t *testing.T) {
	Convey("reconnect", t, func() {
		fakeDialer := &fakes.FakeAMQPDialer{}
		fakeConn := &fakes.FakeAMQPConnection{}
		fakeDialer.DialReturns(fakeConn, nil)

		connDetails := &ConnectionDetails{
			URLs:                     []string{"foobar"},
			dialer:                   fakeDialer,
			maxChannelsPerConnection: maxChannelsPerConnection,
		}

		closeChan := make(chan *amqp.Error)
		fakeMux := &fakes.FakeLocker{}
		conn := &connection{
			details:     connDetails,
			notifyClose: closeChan,
			connMux:     fakeMux,
		}

		Convey("reconnects to rabbit if first try is successful", func() {
			conn.reconnect()
			// mutex is locked
			So(fakeMux.LockCallCount(), ShouldEqual, 1)
			So(fakeMux.UnlockCallCount(), ShouldEqual, 1)
			So(fakeDialer.DialCallCount(), ShouldEqual, 1)
		})

		Convey("will retry connection until successful", func() {
			rand.Seed(time.Now().UnixNano())
			n := rand.Intn(50)
			fakeDialer.DialReturns(nil, errors.New("dial failure"))
			fakeConn := &fakes.FakeAMQPConnection{}
			fakeDialer.DialReturnsOnCall(n, fakeConn, nil)

			conn.reconnect()
			So(fakeDialer.DialCallCount(), ShouldEqual, n+1)
		})
	})
}

func Test_connection_restartAllChannels(t *testing.T) {
	Convey("restartAllChannels", t, func() {
		fakeConnection := &fakes.FakeAMQPConnection{}
		fakeConnection.ChannelReturns(&amqp.Channel{}, nil)
		closeChan := make(chan *amqp.Error)
		fakeConsumerMux := &fakes.FakeRwLocker{}
		fakePublisherMux := &fakes.FakeRwLocker{}
		fakeMux := &fakes.FakeLocker{}
		fakeConsumer := &fakeRestartableConsumer{}
		fakePublisher := &fakeRestartableConsumer{}
		conn := &connection{
			amqpConn:    fakeConnection,
			notifyClose: closeChan,
			connMux:     fakeMux,
			// a single consumer with status cancelled will allow the function
			//  under test to execute, but it will not actually try to restart and error
			consumers:    map[string]restartable{"foo": fakeConsumer},
			publishers:   map[string]restartable{"baz": fakePublisher},
			consumerMux:  fakeConsumerMux,
			publisherMux: fakePublisherMux,
			rmCallback:   func(string) bool { return true },
		}

		Convey("restarts the consumers and publishers", func() {
			fakeConsumer2 := &fakeRestartableConsumer{}
			conn.consumers["bar"] = fakeConsumer2

			err := conn.restartAllChannels()
			So(err, ShouldBeNil)
			So(fakeConsumerMux.LockCallCount(), ShouldEqual, 1)
			So(fakeConsumerMux.UnlockCallCount(), ShouldEqual, 1)
			So(fakePublisherMux.LockCallCount(), ShouldEqual, 1)
			So(fakePublisherMux.UnlockCallCount(), ShouldEqual, 1)
			So(fakeConsumer.RestartCallCount(), ShouldEqual, 1)
			So(fakeConsumer2.RestartCallCount(), ShouldEqual, 1)
			So(fakePublisher.RestartCallCount(), ShouldEqual, 1)
		})

		Convey("skips consumers and publishers that have been cancelled", func() {
			fakeStatus := statusCancelled
			fakeConsumer.GetStatusReturns(fakeStatus)
			fakePublisher.GetStatusReturns(fakeStatus)

			err := conn.restartAllChannels()
			So(err, ShouldBeNil)
			So(fakeConsumer.GetStatusCallCount(), ShouldEqual, 1)
			So(fakeConsumer.RestartCallCount(), ShouldEqual, 0)
			So(fakePublisher.GetStatusCallCount(), ShouldEqual, 1)
			So(fakePublisher.RestartCallCount(), ShouldEqual, 0)
		})

		Convey("errors if channel creation fails", func() {
			fakeConnection.ChannelReturns(nil, errors.New("no channel for you!"))

			err := conn.restartAllChannels()
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "no channel")
		})

		Convey("errors if consumer restart fails", func() {
			fakeChannel := &fakes.FakeAMQPChannel{}
			fakeConnection.ChannelReturns(fakeChannel, nil)
			fakeConsumer.RestartReturns(errors.New("consumer fail"))

			err := conn.restartAllChannels()
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "consumer fail")
			So(fakeChannel.CloseCallCount(), ShouldEqual, 1) // closes the channel if errors
		})

		Convey("errors if publisher restart fails", func() {
			fakeChannel := &fakes.FakeAMQPChannel{}
			fakeConnection.ChannelReturns(fakeChannel, nil)
			fakePublisher.RestartReturns(errors.New("consumer fail"))

			err := conn.restartAllChannels()
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "consumer fail")
			So(fakeChannel.CloseCallCount(), ShouldEqual, 1) // closes the channel if errors
		})

		Convey("when consumer restart fails logs an error if channel close fails", func() {
			capture := captureLogs()
			fakeChannel := &fakes.FakeAMQPChannel{}
			fakeConnection.ChannelReturns(fakeChannel, nil)
			fakeConsumer.RestartReturns(errors.New("consumer fail"))
			fakeChannel.CloseReturns(errors.New("kaboom"))

			err := conn.restartAllChannels()
			So(err, ShouldNotBeNil)
			So(fakeChannel.CloseCallCount(), ShouldEqual, 1)
			So(capture.String(), ShouldContainSubstring, "Attempted to close unused channel after failed restart but got error")
		})

		Convey("when publisher restart fails logs an error if channel close fails", func() {
			capture := captureLogs()
			fakeChannel := &fakes.FakeAMQPChannel{}
			fakeConnection.ChannelReturns(fakeChannel, nil)
			fakePublisher.RestartReturns(errors.New("consumer fail"))
			fakeChannel.CloseReturns(errors.New("kaboom"))

			err := conn.restartAllChannels()
			So(err, ShouldNotBeNil)
			So(fakeChannel.CloseCallCount(), ShouldEqual, 1)
			So(capture.String(), ShouldContainSubstring, "Attempted to close unused channel after failed restart but got error")
		})
	})
}

func captureLogs() *bytes.Buffer {
	logrus.SetLevel(logrus.DebugLevel)
	SetLogger(logrus.StandardLogger().WithField("pkg", "bunny"))
	capture := &bytes.Buffer{}
	logrus.SetOutput(capture)

	return capture
}

func TestInterface(t *testing.T) {
	Convey("the structs meets our interface ", t, func() {
		var _ rwLocker = &sync.RWMutex{}
	})
}
