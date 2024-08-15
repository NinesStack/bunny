package bunny

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/streadway/amqp"

	"github.com/Shimmur/bunny/adapter"
)

type Publisher interface {
	Publish(msg amqp.Publishing) error
	Close() error
}

type publisher struct {
	id             string
	status         *status
	amqpChan       adapter.AMQPChannel
	chanSetupFuncs []SetupFunc
	opts           *PublishOptions
	chanMux        rwLocker
	rmCallback     func(string)
}

type PublishOptions struct {
	ExchangeName string
	RoutingKey   string
	Mandatory    bool
	Immediate    bool
}

func (b *bunny) NewPublisherChannel(opts PublishOptions, setupFuncs ...SetupFunc) (Publisher, error) {
	if b.connections == nil {
		return nil, errors.New("no connection! Must call Connect()")
	}

	id, err := uuid.NewRandom()
	if err != nil {
		return nil, fmt.Errorf("failed to generate ID for publisher")
	}

	// create our representation of the chan to store
	p := &publisher{
		id:             id.String(),
		chanSetupFuncs: setupFuncs,
		opts:           &opts,
		chanMux:        &sync.RWMutex{},
	}

	p.setStatus(statusCreated)

	if err := b.connections.establishPublisherChan(p); err != nil {
		return nil, err
	}

	p.setStatus(statusActive)

	log.Debugf("new publisher created with ID %s", p.id)

	return p, nil
}

func (p *publisher) Publish(msg amqp.Publishing) error {
	_, err := p.publish(msg)
	return err
}

func (p *publisher) PublishWithRetries(msg amqp.Publishing, backoff []time.Duration) error {
	var lastErr error

	// fist try is immediate so prepend the 0
	for i, delay := range append([]time.Duration{0}, backoff...) {
		var retry bool

		// first attempt has no delay so publish first
		retry, lastErr = p.publish(msg)

		// success, return
		if lastErr == nil {
			return nil
		}

		// only retry if a retry makes sense
		if !retry {
			break
		}

		log.Warnf("failed to publish. Retrying attempt %d/%d in %v", i+1, len(backoff), delay)
		time.Sleep(delay)
	}

	if lastErr != nil {
		log.Warnf("all %d retry attempts failed", len(backoff))
		return lastErr
	}

	return nil
}

// internal functionality of publish. Also returns bool to signal if retry should be attempted.
// Shared by single publish and retry publish.
func (p *publisher) publish(msg amqp.Publishing) (bool, error) {
	//  This prevents reuse of publishers in a state other than active, to avoid any
	//  unexpected issues which may be hard to debug
	if p.getStatus() != statusActive {
		return false, fmt.Errorf("Publish() can not be called on consumer in %q state", p.status)
	}

	p.chanMux.RLock()
	defer p.chanMux.RUnlock()

	if err := p.amqpChan.Publish(
		p.opts.ExchangeName,
		p.opts.RoutingKey,
		p.opts.Mandatory,
		p.opts.Immediate,
		msg,
	); err != nil {
		return true, err
	}

	return false, nil
}

func (p *publisher) Close() error {
	if err := p.amqpChan.Close(); err != nil {
		return err
	}

	p.setStatus(statusCancelled)

	// remove itself from publishers
	p.rmCallback(p.id)

	return nil
}

func (p *publisher) restart(ch adapter.AMQPChannel) error {
	log.Debugf("Restarting publisher %s...", p.id)

	// save the new channel
	p.amqpChan = ch

	// TODO: first kill the previous publisher to make sure that we do not leave it dangling

	log.Debugf("calling publisher setup function on restart for publisherID: %s", p.id)

	for _, setupFunc := range p.chanSetupFuncs {
		if err := setupFunc(adapter.UnwrapChannel(ch)); err != nil {
			return fmt.Errorf("failed to setup channel topology on publisher restart: %v", err)
		}
	}

	return nil
}

func (p *publisher) setStatus(st status) {
	if p.status == nil {
		p.status = &st
		return
	}

	atomic.StoreUint32((*uint32)(p.status), uint32(st))
}

func (p *publisher) getStatus() status {
	if p.status == nil {
		return statusCreated // default
	}

	return status(atomic.LoadUint32((*uint32)(p.status)))
}

// required to meet the restartable interface
func (p *publisher) getID() string {
	return p.id
}
