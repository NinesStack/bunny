package bunny

import (
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/streadway/amqp"

	"github.com/Shimmur/bunny/adapter"
)

const (
	maxChannelsPerConnection = 1500
	rebalanceInterval        = time.Minute * 5
)

type connPool struct {
	conns map[string]*connection
	// id of the connection in the pool which should be used
	//  for creating new channels. getNext() will use this
	//  Maybe this could be a pointer to the conn but using
	//  id for now since you would have to lock anyway
	currentID string
	// locked during conn creation process to avoid other threads
	//  from creating more connections
	connPoolMux sync.Locker

	// these are used when creating new connections
	details     *ConnectionDetails
	topologyDef SetupFunc
}

type connection struct {
	id string
	// amqp connection wrapped in our interface to allow dependency injection
	amqpConn    adapter.AMQPConnection
	details     *ConnectionDetails
	topologyDef SetupFunc
	// locked when connection is busy and it should not be used
	connMux sync.Locker

	consumers   map[string]restartable
	consumerMux rwLocker

	publishers   map[string]restartable
	publisherMux rwLocker

	notifyClose chan *amqp.Error

	rmCallback func(string) bool
}

func newPool(details *ConnectionDetails) (*connPool, error) {
	cp := &connPool{
		conns:       map[string]*connection{},
		connPoolMux: &sync.Mutex{},
		details:     details,
	}

	// lock the connection mutex for the entirety of this process to avoid
	//  more than one creation from happening at once
	cp.connPoolMux.Lock()
	defer cp.connPoolMux.Unlock()
	_, err := cp.newConnection()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to rabbitmq: %v", err)
	}

	// kick off rebalancing every 5 minutes
	go cp.startRebalancing(rebalanceInterval)

	return cp, nil
}

// a one time declaration of overall rabbit topology to start
func (c *connPool) declareInitialTopology(setup SetupFunc) error {
	if c.topologyDef != nil {
		return errors.New("can not declare main topology more than once")
	}

	c.topologyDef = setup

	conn, err := c.getNext()
	if err != nil {
		return fmt.Errorf("failed to obtain connection for declaring initial topology: %v", err)
	}

	conn.topologyDef = setup

	if err := conn.declareTopology(); err != nil {
		return err
	}

	return nil
}

// a helper that is reusable for restarts
func (c *connection) declareTopology() error {
	ch, err := c.amqpConn.Channel()
	if err != nil {
		return fmt.Errorf("failed to create channcel to define topology: %v", err)
	}

	// always try to close this, even if there is an error
	defer func() {
		if err := ch.Close(); err != nil {
			// log this but allow execution to continue
			log.Errorf("failed to close channel for topology declaration: %v", err)
		}
	}()

	if err := c.topologyDef(adapter.UnwrapChannel(ch)); err != nil {
		return err
	}

	return nil
}

// always lock the mutex before calling this method
// TODO: ^ this is not great. find a way to do the mutex so we can lock
//  in here too without allowing a race condition
func (c *connPool) newConnection() (*connection, error) {
	// protect against nil
	if c.conns == nil {
		c.conns = map[string]*connection{}
	}

	log.Debug("Creating new connection...")
	id, err := uuid.NewRandom()
	if err != nil {
		return nil, fmt.Errorf("failed to generate ID for connection: %v", err)
	}

	conn := &connection{
		id:          id.String(),
		details:     c.details,
		topologyDef: c.topologyDef,
		connMux:     &sync.Mutex{},

		consumers:   map[string]restartable{},
		consumerMux: &sync.RWMutex{},

		publishers:   map[string]restartable{},
		publisherMux: &sync.RWMutex{},

		rmCallback: c.deleteConnection,
	}

	// TODO: add retry logic here
	if err := conn.connect(); err != nil {
		return nil, err
	}

	// this is safe because this whole method is wrapped in a lock
	c.conns[conn.id] = conn
	c.currentID = conn.id

	return conn, nil
}

func (c *connection) connect() error {
	// safeguard against missing details
	if c.details == nil {
		// initial connection was never established, need to bail
		return errors.New("no connection details found, must connect() first")
	}

	var (
		amqpConn adapter.AMQPConnection
		err      error
	)

	// TODO: url parse and sanitize
	log.Debugf("dialing rabbit... %d urls to try", len(c.details.URLs))

	if len(c.details.URLs) < 1 {
		return errors.New("no AMQP URLs were supplied")
	}

	for i, url := range c.details.URLs {
		var tlsConfig *tls.Config

		if c.details.UseTLS {
			log.Debugf("Dialling url %d using TLS... verify certificate: %v", i, !c.details.SkipVerifyTLS)
			tlsConfig = &tls.Config{}

			if c.details.SkipVerifyTLS {
				tlsConfig.InsecureSkipVerify = true
			}
		} else {
			log.Debugf("Dialling url %d without TLS...", i)
		}

		amqpConn, err = c.details.dialer.Dial(url, tlsConfig)
		if err == nil {
			log.Debug("connection successful")
			break
		}

		log.Errorf("failed to connect to URL %d: %v", i, err)
	}
	if err != nil {
		return fmt.Errorf("failed to connect to all URLs. Last error: %v", err)
	}

	// save the connection. wrap it in our interface to allow dependency injection
	c.amqpConn = amqpConn

	// Watch for closed connection
	c.notifyClose = c.amqpConn.NotifyClose(make(chan *amqp.Error))
	go c.watchNotifyClose()

	return nil
}

func (c *connPool) getNext() (*connection, error) {
	if c.details == nil {
		return nil, errors.New("missing connection details, must connect() first")
	}

	// Lock the connection mutex for the entirety of this process to avoid
	//  more than one creation from happening at once
	c.connPoolMux.Lock()
	defer c.connPoolMux.Unlock()

	// check that there is at least one connection
	if len(c.conns) > 0 {
		// Grab the latest connection for now
		// TODO: do something better here to reuse older connections that have capacity
		con, ok := c.conns[c.currentID]

		// check that this connection exists and see that there is capacity on it
		if ok && con.numChannels() < c.details.maxChannelsPerConnection {
			// has capacity, so return it and release the lock
			return con, nil
		}

		log.Debugf("reached max number of channels per connection: %d, starting new...", c.details.maxChannelsPerConnection)
	}

	// **At this point we did not find any connections, or the one we got has no more
	// capacity, so we must create a new connection**

	// safe to create here because we still have the lock
	newCon, err := c.newConnection()
	if err != nil {
		return nil, fmt.Errorf("need new connection but failed to create: %v", err)
	}

	return newCon, nil
}

func (c *connPool) startRebalancing(interval time.Duration) {
	for range time.Tick(interval) {
		c.rebalance()
	}
}

// This will "rebalance" the connections by choosing which connection gets more channels next
// Naive, very basic, and potentially expensive way of doing this but ok for an MVP as long as
// it doesn't run very often
func (c *connPool) rebalance() {
	c.connPoolMux.Lock()
	defer c.connPoolMux.Unlock()

	log.Debugf("Rebalancing connection pool... size: %d", len(c.conns))

	var (
		min    = maxChannelsPerConnection
		chosen string
	)

	// loop through all the connections and find the one with least consumers
	for _, conn := range c.conns {
		n := conn.numChannels()
		if n < min {
			min = n
			chosen = conn.id
		}
	}

	// if one was chosen, set it
	if chosen != "" {
		c.currentID = chosen
	}
}

func (c *connPool) deleteConnection(id string) bool {
	log.Debugf("Removing connection %s from pool...", id)

	c.connPoolMux.Lock()
	defer c.connPoolMux.Unlock()

	// if there is only one connection left, do not allow it to be deleted. This is safe
	//  because we have obtained a lock on the pool
	if len(c.conns) == 1 {
		log.Debug("Last connection in pool can not be deleted")
		return false
	}

	delete(c.conns, id)

	return true
}

// Total number of channels on this connection
// There is one consumer or publisher per channel, so count those.
func (c *connection) numChannels() int {
	c.consumerMux.RLock()
	defer c.consumerMux.RUnlock()

	c.publisherMux.RLock()
	defer c.publisherMux.RUnlock()

	return len(c.consumers) + len(c.publishers)
}

/*-----------
| Consumers |
-----------*/

// A helper to establish the consumer channel. A shared implementation used in connect and reconnect
func (c *connPool) establishConsumerChan(consumer *consumer) error {
	// TODO: put in a configurable rate limit that will be global so we dont take down rabbit
	conn, err := c.getNext()
	if err != nil {
		return fmt.Errorf("cannot establish consumer channel: %v", err)
	}

	log.Debug("establishing a channel...")

	// establish a channel
	ch, err := conn.amqpConn.Channel()
	if err != nil {
		return fmt.Errorf("failed to initialize channel: %v", err)
	}

	// register the consumer
	conn.registerConsumer(consumer)

	// This should be safe because we would not be establishing a chan if
	//  it is in active use
	consumer.amqpChan = ch
	consumer.rmCallback = conn.deleteConsumer

	log.Debug("running channel topology setup func...")
	// run user provided topology setup if provided
	for _, setupFunc := range consumer.chanSetupFuncs {
		if err := setupFunc(adapter.UnwrapChannel(ch)); err != nil {
			return fmt.Errorf("failed to setup channel with provided func: %v", err)
		}
	}

	return nil
}

func (c *connection) registerConsumer(consumer *consumer) {
	c.consumerMux.Lock()
	defer c.consumerMux.Unlock()

	c.consumers[consumer.id] = consumer
}

func (c *connection) deleteConsumer(id string) {
	log.Debugf("Removing consumer %s from connection...", id)
	c.consumerMux.Lock()
	defer c.consumerMux.Unlock()

	delete(c.consumers, id)
}

/*------------
| Publishers |
------------*/

// A helper to establish the publisher channel. A shared implementation used in connect and reconnect
func (c *connPool) establishPublisherChan(pub *publisher) error {
	// TODO: put in a configurable rate limit that will be global so we dont take down rabbit
	conn, err := c.getNext()
	if err != nil {
		return fmt.Errorf("cannot establish publisher channel: %v", err)
	}

	log.Debug("establishing a channel...")

	// establish a channel
	ch, err := conn.amqpConn.Channel()
	if err != nil {
		return fmt.Errorf("failed to initialize channel: %v", err)
	}

	// register the consumer
	conn.registerPublisher(pub)

	// This should be safe because we would not be establishing a chan if
	//  it is in active use
	pub.amqpChan = ch
	pub.rmCallback = conn.deletePublisher

	log.Debug("running channel topology setup func...")
	// run user provided topology setup if provided
	for _, setupFunc := range pub.chanSetupFuncs {
		if err := setupFunc(adapter.UnwrapChannel(ch)); err != nil {
			return fmt.Errorf("failed to setup channel with provided func: %v", err)
		}
	}

	return nil
}

func (c *connection) registerPublisher(pub *publisher) {
	c.publisherMux.Lock()
	defer c.publisherMux.Unlock()

	c.publishers[pub.id] = pub
}

func (c *connection) deletePublisher(id string) {
	log.Debugf("Removing publisher %s from connection...", id)
	c.publisherMux.Lock()
	defer c.publisherMux.Unlock()

	delete(c.publishers, id)
}

/*-----------------
| Reconnect Logic |
-----------------*/

// Do not like that this fake is generated into the same dir, but need to make
//  an exception here to make this work. The alternative is to make the status
//  type exported, but that is not necessarily something that we want.
//  Also note that this fake gets generated with the package name (ie bunny.status)
//  so you will need to find and replace to fix. That also sucks and seems like a bug in
//  counterfeiter. TODO: Revisit this to see if it can be done better
// //go:generate counterfeiter -o fake_restartable_test.go . restartable

// restartable interface is used by the connection and allows for mocking
//  of the consumer functionality in tests
type restartable interface {
	restart(ch adapter.AMQPChannel) error
	getID() string
	getStatus() status
}

func (c *connection) watchNotifyClose() {
	log.Debug("subscribing to connection close notifications")

	watchBeginTime := time.Now()

	// watch for close notification, reconnect, repeat
	closeErr := <-c.notifyClose
	log.Debugf("received message on notify close channel: %+v", closeErr)
	log.Warnf("Detected connection close after %v. Reconnecting...", time.Since(watchBeginTime))

	// this will block until connection is established and restart is complete
	c.restart()
}

// restart will reestablish a connection and restart all the components of the
//  connection that were previously running
func (c *connection) restart() {
	// first reconnect
	reconnectBeginTime := time.Now()

	if c.numChannels() < 1 {
		log.Infof("Connection %s has no channels. Not restarting connection...", c.id)

		// attempt to remove it from the pool. If unsuccessful, continue with restart. This
		//  prevents the deletion of the last remaining connection in the pool
		if c.rmCallback(c.id) {
			return
		}
	}

	c.reconnect()

	// if there is a topology definition, redeclare it
	if c.topologyDef != nil {
		log.Debug("Re-declaring initial topology...")

		if err := c.declareTopology(); err != nil {
			log.Errorf("could not declare topology on restart: %v", err)
			// TODO: handle this better (see comment below)
			// For now just allow execution to continue because the redeclare should be idempotent
			// If there were things declared as auto-delete, this will be a problem
		}
	}

	if err := c.restartAllChannels(); err != nil {
		log.Errorf("Failed to restart consumers! %v", err)
		// TODO: handle this better (see comment below)
		return
	}

	// TODO: maybe if there are errors on restart, we disconnect and try again.
	//  Log lots of errors so someone will notice? Maybe we offer a "fatal chan" so
	//  we can let the user know that shit has hit the fan... Or maybe we give the
	//  user an option what we do. Log, err chan? Panic is a bad choice generally,
	//  but even worse in this case because it's probably running in a goroutine, so
	//  we would just die and no one would know about it.

	log.Infof("Successfully reconnected after %v", time.Since(reconnectBeginTime))
}

// helper function for testability
func (c *connection) reconnect() {
	// lock so that other functions can not attempt to use the connection
	c.connMux.Lock()
	defer c.connMux.Unlock()

	// TODO will we still drop messages here?

	var attempts int

	beginTime := time.Now()

	// TODO: implement exponential backoff
	for {
		log.Warnf("Attempting to reconnect. All processing has been blocked for %v", time.Since(beginTime))

		attempts++
		if err := c.connect(); err != nil {
			log.Warnf("failed attempt %d to reconnect: %s; retrying in %d seconds", attempts, err, c.details.RetryReconnectSec)
			time.Sleep(time.Duration(c.details.RetryReconnectSec) * time.Second)
			continue
		}
		log.Debugf("successfully reconnected after %d attempts in %v", attempts, time.Since(beginTime))
		break
	}

	return
}

// helper function for testability and use of defers
func (c *connection) restartAllChannels() error {
	//  Lock these so that we do not add more while restarting
	c.publisherMux.Lock()
	defer c.publisherMux.Unlock()

	c.consumerMux.Lock()
	defer c.consumerMux.Unlock()

	// quick and dirty error aggregation
	var errs []string

	// TODO: implement some kind of rate limiting. Maybe something global, not just restarts

	log.Debug("Restarting all existing publishers...")

	for _, pub := range c.publishers {
		if err := c.restartChild(pub); err != nil {
			// TODO: not sure about how we want to handle these...
			//  restart as many as we can, or fail fast when one errors?
			errs = append(errs, fmt.Sprintf("failed to restart publisher %s: %v", pub.getID(), err))
		}
	}

	log.Debug("Restarting all existing consumers...")

	for _, cons := range c.consumers {
		if err := c.restartChild(cons); err != nil {
			// TODO: here too...
			errs = append(errs, fmt.Sprintf("failed to restart consumer %s: %v", cons.getID(), err))
		}
	}

	// aggregate errors for now and do this better in the future
	if len(errs) > 0 {
		return fmt.Errorf("failures during channel restarts: %v", strings.Join(errs, ", "))
	}

	return nil
}

func (c *connection) restartChild(r restartable) error {
	// Do not restart children that have been cancelled. This avoids a race condition where
	//  the child ended but had not been deleted before a restart was triggered.
	if r.getStatus() == statusCancelled {
		log.Debugf("%s is cancelled so not restarting", r.getID())
		return nil
	}

	// create the exclusive channel that is associated with this child
	ch, err := c.amqpConn.Channel()
	if err != nil {
		log.Errorf("Failed to create a new channel: %v", err)
		return err
	}

	if err := r.restart(ch); err != nil {
		log.Errorf("Failed to restart %s: %v", r.getID(), err)

		// need to close the channel we created for this child
		if err := ch.Close(); err != nil {
			// Unfortunately this is the best we can do. Probably a lot of bad things are already happening
			//  if this fails and someone will know.
			log.Errorf("Attempted to close unused channel after failed restart but got error: %v", err)
		}

		return err
	}

	return nil
}

/*-------------------
| Helper Interfaces |
-------------------*/

//go:generate counterfeiter -o fakes/fake_rwLocker.go . rwLocker
//go:generate counterfeiter -o fakes/fake_locker.go sync.Locker

type rwLocker interface {
	RLock()
	RUnlock()
	Lock()
	Unlock()
}
