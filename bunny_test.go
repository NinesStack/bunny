package bunny

import (
	"errors"
	"sync"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/streadway/amqp"

	"github.com/Shimmur/bunny/adapter"
	"github.com/Shimmur/bunny/fakes"
)

func TestNewBunny(t *testing.T) {
	Convey("NewBunny()", t, func() {
		details := ConnectionDetails{
			URLs: []string{"foobar"},
		}

		Convey("meets the interface", func() {
			bunn := NewRabbit(details)
			var _ Rabbit = bunn
		})

		Convey("Sets the connection details", func() {
			b := NewRabbit(details)
			So(b.connDetails, ShouldNotBeNil)
			So(b.connDetails.URLs, ShouldResemble, details.URLs)
			So(b.connDetails.maxChannelsPerConnection, ShouldEqual, maxChannelsPerConnection)
			So(b.connDetails.dialer, ShouldHaveSameTypeAs, &adapter.Dialer{})
		})
	})
}

func TestConnect(t *testing.T) {
	Convey("Connect()", t, func() {
		bunn := NewRabbit(ConnectionDetails{
			URLs: []string{"foobar"},
		})

		fakeDialer := &fakes.FakeAMQPDialer{}
		fakeConn := &fakes.FakeAMQPConnection{}
		fakeDialer.DialReturns(fakeConn, nil)

		bunn.connDetails.dialer = fakeDialer

		Convey("connects to rabbit", func() {
			err := bunn.Connect()

			So(err, ShouldBeNil)
			So(bunn.connections, ShouldNotBeNil)
			So(bunn.connections.conns, ShouldHaveLength, 1)
		})

		Convey("errors if called twice", func() {
			err := bunn.Connect()
			So(err, ShouldBeNil)

			err = bunn.Connect()
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldEqual, "connect may only be called once")
			So(bunn.connections, ShouldNotBeNil)
			So(bunn.connections.conns, ShouldHaveLength, 1)
		})

		Convey("errors if connect fails", func() {
			fakeDialer.DialReturns(nil, errors.New("did not connect"))

			err := bunn.Connect()
			So(err, ShouldNotBeNil)
			So(err.Error(), ShouldContainSubstring, "did not connect")
		})
	})
}

func Test_bunny_DeclareTopology(t *testing.T) {
	Convey("DeclareTopology", t, func() {
		fakeConnection := &fakes.FakeAMQPConnection{}
		fakeChannel := &fakes.FakeAMQPChannel{}
		fakeConnection.ChannelReturns(fakeChannel, nil)

		bunn := &bunny{
			connections: &connPool{
				conns: map[string]*connection{
					"conn": &connection{
						amqpConn:     fakeConnection,
						consumerMux:  &sync.RWMutex{},
						publisherMux: &sync.RWMutex{},
					},
				},
				currentID: "conn",
				details: &ConnectionDetails{
					maxChannelsPerConnection: maxChannelsPerConnection,
				},
				connPoolMux: &sync.Mutex{},
			},
		}

		Convey("all user provided setup funcs are called", func() {
			var first, second, third bool
			setupFuncs := []SetupFunc{
				func(ch *amqp.Channel) error { first = true; return nil },
				func(ch *amqp.Channel) error { second = true; return nil },
				func(ch *amqp.Channel) error { third = true; return nil },
			}

			err := bunn.DeclareTopology(setupFuncs...)
			So(err, ShouldBeNil)
			So(first, ShouldBeTrue)
			So(second, ShouldBeTrue)
			So(third, ShouldBeTrue)
		})

		Convey("errors if one setup func errors and stops execution", func() {
			var first, second, third bool
			setupFuncs := []SetupFunc{
				func(ch *amqp.Channel) error { first = true; return nil },
				func(ch *amqp.Channel) error { return errors.New("boom") },
				func(ch *amqp.Channel) error { third = true; return nil },
			}

			err := bunn.DeclareTopology(setupFuncs...)
			So(err, ShouldNotBeNil)
			So(first, ShouldBeTrue)
			So(second, ShouldBeFalse)
			So(third, ShouldBeFalse)
		})
	})
}
