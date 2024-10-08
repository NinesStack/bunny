package bunny

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/streadway/amqp"

	"github.com/Shimmur/bunny/adapter"
)

type Consumer interface {
	Consume(consumeFunc ConsumeFunc, opts ConsumeOptions, errs chan<- error) error
	Cancel(noWait bool) error
}

// Function that is run against evey item delivered from rabbit
//  Author of this function is responsible for calling Delivery.Ack()
type ConsumeFunc func(msg *amqp.Delivery) error

type ConsumeOptions struct {
	QueueName  string
	AutoAck    bool
	Exclusive  bool
	NoWait     bool
	QoSOptions QoSOptions
}

type QoSOptions struct {
	PrefetchCount int
	PrefetchSize  int
	Global        bool
}

type status uint32

const (
	statusCreated status = iota
	statusActive
	statusCancelled
)

func (s status) String() string {
	switch s {
	case statusCreated:
		return "created"
	case statusActive:
		return "active"
	case statusCancelled:
		return "cancelled"
	default:
		return "unknown"
	}
}

type consumer struct {
	id             string
	status         *status
	amqpChan       adapter.AMQPChannel
	chanSetupFuncs []SetupFunc
	consumeFunc    ConsumeFunc
	consumerTag    string
	opts           *ConsumeOptions
	deliveryChan   <-chan amqp.Delivery
	errorChan      chan<- error
	deliveryMux    rwLocker
	rmCallback     func(string)
}

func (b *bunny) NewConsumerChannel(setupFuncs ...SetupFunc) (Consumer, error) {
	if b.connections == nil {
		return nil, errors.New("no connection! Must call Connect()")
	}

	id, err := uuid.NewRandom()
	if err != nil {
		return nil, fmt.Errorf("failed to generate ID for consumer")
	}

	// create our representation of the consumer chan to store
	c := &consumer{
		id:             id.String(),
		chanSetupFuncs: setupFuncs,
		deliveryMux:    &sync.RWMutex{},
	}

	c.setStatus(statusCreated)

	if err := b.connections.establishConsumerChan(c); err != nil {
		return nil, err
	}

	log.Debugf("new consumer created with ID %s", c.id)

	return c, nil
}

// exported version of consume for the user to kick of consumption
func (c *consumer) Consume(consumeFunc ConsumeFunc, opts ConsumeOptions, errs chan<- error) error {
	// Enforce one consumer per channel, and also prevent consuming on cancelled
	//  This also prevents reuse of cancelled consumers to avoid any unexpected issues
	//  which may be hard to debug
	if c.getStatus() != statusCreated {
		return fmt.Errorf("Consume() can not be called on consumer in %q state", c.status)
	}

	c.consumeFunc = consumeFunc
	c.opts = &opts
	c.errorChan = errs

	err := c.amqpChan.Qos(c.opts.QoSOptions.PrefetchCount, c.opts.QoSOptions.PrefetchSize, c.opts.QoSOptions.Global)
	if err != nil {
		return fmt.Errorf("failed to set qos options: %v", err)
	}

	log.Debugf("Starting consumer %s...", c.id)

	if err := c.consume(); err != nil {
		return err
	}

	return nil
}

// internal consume that is reusable for restarts
func (c *consumer) consume() error {
	// generate a consumer tag to ensure uniqueness
	id, err := uuid.NewRandom()
	if err != nil {
		return fmt.Errorf("failed to generate consumer tag: %v", err)
	}

	// the consumer tag is kept separate from the consumer ID because the consumer ID
	//  will survive restarts, but the consumer tag must not.
	c.consumerTag = id.String()

	log.Debugf("setting up to consume from queue %s with consumer tag %s...", c.opts.QueueName, c.consumerTag)

	deliveries, err := c.amqpChan.Consume(
		c.opts.QueueName,
		c.consumerTag,
		c.opts.AutoAck,
		c.opts.Exclusive,
		false, // noLocal is not supported by Rabbit
		c.opts.NoWait,
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to begin consuming from channel: %v consumer tag: %s", err, c.consumerTag)
	}

	c.setStatus(statusActive)

	log.Debugf("kicking off consumer func for consumer tag %s", c.consumerTag)

	// must lock here because in the restart case, we may still be consuming from it
	c.deliveryMux.Lock()
	c.deliveryChan = deliveries
	c.deliveryMux.Unlock()

	go c.consumeLoop()

	log.Debugf("consuming from queue %s with consumer tag %s", c.opts.QueueName, c.consumerTag)

	return nil
}

// returned bool is for testing purposes
func (c *consumer) consumeLoop() bool {
	// TODO: take ctx code from Rabbit lib for cancels
	for {
		// TODO: this should not continue to consume if we restarted. Will there be two?
		//  should use a select here and have a close/cancel chan as well
		item, ok := <-c.deliveries()
		if !ok {
			log.Debugf("got delivery channel close! consumer tag: %s", c.consumerTag)
			// just exit this goroutine and allow another one to be
			//  spawned on a new delivery chan
			return true
		}

		if err := c.consumeFunc(&item); err != nil {
			log.Debugf("error during consume: %s", err)
			if c.errorChan != nil {
				c.errorChan <- err
			}
		}
	}
}

func (c *consumer) Cancel(noWait bool) error {
	closed := false
	// This will trigger a close of the delivery channel and stop the consumer loop as well
	if err := c.amqpChan.Cancel(c.consumerTag, noWait); err != nil {
		if err != amqp.ErrClosed {
			return err
		}

		// we got an ErrClosed so its already closed
		closed = true
	}

	c.setStatus(statusCancelled)

	// remove itself from consumers
	c.rmCallback(c.id)

	// cleanup the channel if not closed already
	if !closed {
		if err := c.amqpChan.Close(); err != nil {
			return fmt.Errorf("failed to close channel for consumer ID %s: %v", c.consumerTag, err)
		}
	}

	return nil
}

// helper wrapper for Consume in the restart case
func (c *consumer) restart(ch adapter.AMQPChannel) error {
	log.Debugf("Restarting consumer %s...", c.id)

	// save the new channel
	c.amqpChan = ch

	// TODO: first kill the previous consumer to make sure that we do not leave it dangling

	log.Debugf("calling consumer setup function on restart for consumerID: %s", c.id)

	// run the setup func if it's not nil
	for _, setupFunc := range c.chanSetupFuncs {
		if err := setupFunc(adapter.UnwrapChannel(ch)); err != nil {
			return fmt.Errorf("failed to setup channel topology on restart: %v", err)
		}
	}

	if err := c.consume(); err != nil {
		return fmt.Errorf("failed to begin consuming from channel on consumer restart: %v", err)
	}

	return nil
}

func (c *consumer) setStatus(st status) {
	if c.status == nil {
		c.status = &st
		return
	}

	atomic.StoreUint32((*uint32)(c.status), uint32(st))
}

func (c *consumer) getStatus() status {
	if c.status == nil {
		return statusCreated // default
	}

	return status(atomic.LoadUint32((*uint32)(c.status)))
}

// required to meet the restartable interface
func (c *consumer) getID() string {
	return c.id
}

func (c *consumer) deliveries() <-chan amqp.Delivery {
	c.deliveryMux.RLock()
	defer c.deliveryMux.RUnlock()
	return c.deliveryChan
}
