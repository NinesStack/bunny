// Code generated by counterfeiter. DO NOT EDIT.
package fakes

import (
	"sync"

	"github.com/Shimmur/bunny/adapter"
	"github.com/streadway/amqp"
)

type FakeAMQPConnection struct {
	ChannelStub        func() (adapter.AMQPChannel, error)
	channelMutex       sync.RWMutex
	channelArgsForCall []struct {
	}
	channelReturns struct {
		result1 adapter.AMQPChannel
		result2 error
	}
	channelReturnsOnCall map[int]struct {
		result1 adapter.AMQPChannel
		result2 error
	}
	CloseStub        func() error
	closeMutex       sync.RWMutex
	closeArgsForCall []struct {
	}
	closeReturns struct {
		result1 error
	}
	closeReturnsOnCall map[int]struct {
		result1 error
	}
	NotifyCloseStub        func(chan *amqp.Error) chan *amqp.Error
	notifyCloseMutex       sync.RWMutex
	notifyCloseArgsForCall []struct {
		arg1 chan *amqp.Error
	}
	notifyCloseReturns struct {
		result1 chan *amqp.Error
	}
	notifyCloseReturnsOnCall map[int]struct {
		result1 chan *amqp.Error
	}
	invocations      map[string][][]interface{}
	invocationsMutex sync.RWMutex
}

func (fake *FakeAMQPConnection) Channel() (adapter.AMQPChannel, error) {
	fake.channelMutex.Lock()
	ret, specificReturn := fake.channelReturnsOnCall[len(fake.channelArgsForCall)]
	fake.channelArgsForCall = append(fake.channelArgsForCall, struct {
	}{})
	stub := fake.ChannelStub
	fakeReturns := fake.channelReturns
	fake.recordInvocation("Channel", []interface{}{})
	fake.channelMutex.Unlock()
	if stub != nil {
		return stub()
	}
	if specificReturn {
		return ret.result1, ret.result2
	}
	return fakeReturns.result1, fakeReturns.result2
}

func (fake *FakeAMQPConnection) ChannelCallCount() int {
	fake.channelMutex.RLock()
	defer fake.channelMutex.RUnlock()
	return len(fake.channelArgsForCall)
}

func (fake *FakeAMQPConnection) ChannelCalls(stub func() (adapter.AMQPChannel, error)) {
	fake.channelMutex.Lock()
	defer fake.channelMutex.Unlock()
	fake.ChannelStub = stub
}

func (fake *FakeAMQPConnection) ChannelReturns(result1 adapter.AMQPChannel, result2 error) {
	fake.channelMutex.Lock()
	defer fake.channelMutex.Unlock()
	fake.ChannelStub = nil
	fake.channelReturns = struct {
		result1 adapter.AMQPChannel
		result2 error
	}{result1, result2}
}

func (fake *FakeAMQPConnection) ChannelReturnsOnCall(i int, result1 adapter.AMQPChannel, result2 error) {
	fake.channelMutex.Lock()
	defer fake.channelMutex.Unlock()
	fake.ChannelStub = nil
	if fake.channelReturnsOnCall == nil {
		fake.channelReturnsOnCall = make(map[int]struct {
			result1 adapter.AMQPChannel
			result2 error
		})
	}
	fake.channelReturnsOnCall[i] = struct {
		result1 adapter.AMQPChannel
		result2 error
	}{result1, result2}
}

func (fake *FakeAMQPConnection) Close() error {
	fake.closeMutex.Lock()
	ret, specificReturn := fake.closeReturnsOnCall[len(fake.closeArgsForCall)]
	fake.closeArgsForCall = append(fake.closeArgsForCall, struct {
	}{})
	stub := fake.CloseStub
	fakeReturns := fake.closeReturns
	fake.recordInvocation("Close", []interface{}{})
	fake.closeMutex.Unlock()
	if stub != nil {
		return stub()
	}
	if specificReturn {
		return ret.result1
	}
	return fakeReturns.result1
}

func (fake *FakeAMQPConnection) CloseCallCount() int {
	fake.closeMutex.RLock()
	defer fake.closeMutex.RUnlock()
	return len(fake.closeArgsForCall)
}

func (fake *FakeAMQPConnection) CloseCalls(stub func() error) {
	fake.closeMutex.Lock()
	defer fake.closeMutex.Unlock()
	fake.CloseStub = stub
}

func (fake *FakeAMQPConnection) CloseReturns(result1 error) {
	fake.closeMutex.Lock()
	defer fake.closeMutex.Unlock()
	fake.CloseStub = nil
	fake.closeReturns = struct {
		result1 error
	}{result1}
}

func (fake *FakeAMQPConnection) CloseReturnsOnCall(i int, result1 error) {
	fake.closeMutex.Lock()
	defer fake.closeMutex.Unlock()
	fake.CloseStub = nil
	if fake.closeReturnsOnCall == nil {
		fake.closeReturnsOnCall = make(map[int]struct {
			result1 error
		})
	}
	fake.closeReturnsOnCall[i] = struct {
		result1 error
	}{result1}
}

func (fake *FakeAMQPConnection) NotifyClose(arg1 chan *amqp.Error) chan *amqp.Error {
	fake.notifyCloseMutex.Lock()
	ret, specificReturn := fake.notifyCloseReturnsOnCall[len(fake.notifyCloseArgsForCall)]
	fake.notifyCloseArgsForCall = append(fake.notifyCloseArgsForCall, struct {
		arg1 chan *amqp.Error
	}{arg1})
	stub := fake.NotifyCloseStub
	fakeReturns := fake.notifyCloseReturns
	fake.recordInvocation("NotifyClose", []interface{}{arg1})
	fake.notifyCloseMutex.Unlock()
	if stub != nil {
		return stub(arg1)
	}
	if specificReturn {
		return ret.result1
	}
	return fakeReturns.result1
}

func (fake *FakeAMQPConnection) NotifyCloseCallCount() int {
	fake.notifyCloseMutex.RLock()
	defer fake.notifyCloseMutex.RUnlock()
	return len(fake.notifyCloseArgsForCall)
}

func (fake *FakeAMQPConnection) NotifyCloseCalls(stub func(chan *amqp.Error) chan *amqp.Error) {
	fake.notifyCloseMutex.Lock()
	defer fake.notifyCloseMutex.Unlock()
	fake.NotifyCloseStub = stub
}

func (fake *FakeAMQPConnection) NotifyCloseArgsForCall(i int) chan *amqp.Error {
	fake.notifyCloseMutex.RLock()
	defer fake.notifyCloseMutex.RUnlock()
	argsForCall := fake.notifyCloseArgsForCall[i]
	return argsForCall.arg1
}

func (fake *FakeAMQPConnection) NotifyCloseReturns(result1 chan *amqp.Error) {
	fake.notifyCloseMutex.Lock()
	defer fake.notifyCloseMutex.Unlock()
	fake.NotifyCloseStub = nil
	fake.notifyCloseReturns = struct {
		result1 chan *amqp.Error
	}{result1}
}

func (fake *FakeAMQPConnection) NotifyCloseReturnsOnCall(i int, result1 chan *amqp.Error) {
	fake.notifyCloseMutex.Lock()
	defer fake.notifyCloseMutex.Unlock()
	fake.NotifyCloseStub = nil
	if fake.notifyCloseReturnsOnCall == nil {
		fake.notifyCloseReturnsOnCall = make(map[int]struct {
			result1 chan *amqp.Error
		})
	}
	fake.notifyCloseReturnsOnCall[i] = struct {
		result1 chan *amqp.Error
	}{result1}
}

func (fake *FakeAMQPConnection) Invocations() map[string][][]interface{} {
	fake.invocationsMutex.RLock()
	defer fake.invocationsMutex.RUnlock()
	fake.channelMutex.RLock()
	defer fake.channelMutex.RUnlock()
	fake.closeMutex.RLock()
	defer fake.closeMutex.RUnlock()
	fake.notifyCloseMutex.RLock()
	defer fake.notifyCloseMutex.RUnlock()
	copiedInvocations := map[string][][]interface{}{}
	for key, value := range fake.invocations {
		copiedInvocations[key] = value
	}
	return copiedInvocations
}

func (fake *FakeAMQPConnection) recordInvocation(key string, args []interface{}) {
	fake.invocationsMutex.Lock()
	defer fake.invocationsMutex.Unlock()
	if fake.invocations == nil {
		fake.invocations = map[string][][]interface{}{}
	}
	if fake.invocations[key] == nil {
		fake.invocations[key] = [][]interface{}{}
	}
	fake.invocations[key] = append(fake.invocations[key], args)
}

var _ adapter.AMQPConnection = new(FakeAMQPConnection)
