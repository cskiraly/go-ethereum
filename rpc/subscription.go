// Copyright 2016 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package rpc

import (
	"container/list"
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"time"
)

var (
	// ErrNotificationsUnsupported is returned by the client when the connection doesn't
	// support notifications. You can use this error value to check for subscription
	// support like this:
	//
	//	sub, err := client.EthSubscribe(ctx, channel, "newHeads", true)
	//	if errors.Is(err, rpc.ErrNotificationsUnsupported) {
	//		// Server does not support subscriptions, fall back to polling.
	//	}
	//
	ErrNotificationsUnsupported = notificationsUnsupportedError{}

	// ErrSubscriptionNotFound is returned when the notification for the given id is not found
	ErrSubscriptionNotFound = errors.New("subscription not found")
)

var globalGen = randomIDGenerator()

// ID defines a pseudo random number that is used to identify RPC subscriptions.
type ID string

// NewID returns a new, random ID.
func NewID() ID {
	return globalGen()
}

// randomIDGenerator returns a function generates a random IDs.
func randomIDGenerator() func() ID {
	var buf = make([]byte, 8)
	var seed int64
	if _, err := crand.Read(buf); err == nil {
		seed = int64(binary.BigEndian.Uint64(buf))
	} else {
		seed = int64(time.Now().Nanosecond())
	}

	var (
		mu  sync.Mutex
		rng = rand.New(rand.NewSource(seed))
	)
	return func() ID {
		mu.Lock()
		defer mu.Unlock()
		id := make([]byte, 16)
		rng.Read(id)
		return encodeID(id)
	}
}

func encodeID(b []byte) ID {
	id := hex.EncodeToString(b)
	id = strings.TrimLeft(id, "0")
	if id == "" {
		id = "0" // ID's are RPC quantities, no leading zero's and 0 is 0x0.
	}
	return ID("0x" + id)
}

type notifierKey struct{}

// NotifierFromContext returns the Notifier value stored in ctx, if any.
func NotifierFromContext(ctx context.Context) (*Notifier, bool) {
	n, ok := ctx.Value(notifierKey{}).(*Notifier)
	return n, ok
}

// Notifier is tied to an RPC connection that supports subscriptions.
// Server callbacks use the notifier to send notifications.
type Notifier struct {
	h         *handler
	namespace string

	mu           sync.Mutex
	sub          *Subscription
	buffer       []any
	callReturned bool
	activated    bool
	// failed is set when a notification write fails (broken connection,
	// write deadline exceeded). Once set, future Notify calls return
	// errSubscriptionClosed immediately so a wedged client can't keep
	// the per-subscription goroutine bouncing off the 10-second write
	// timeout for every queued event. The subscription's err channel
	// is closed and the underlying codec is closed; the latter triggers
	// the normal connection-shutdown path which cleans up all other
	// subscriptions tied to the same connection.
	failed bool
}

// errSubscriptionClosed is returned from Notify after a write failure
// has marked the notifier as failed. Mirrors what callers see when the
// subscription is canceled normally.
var errSubscriptionClosed = errors.New("subscription closed")

// CreateSubscription returns a new subscription that is coupled to the
// RPC connection. By default subscriptions are inactive and notifications
// are dropped until the subscription is marked as active. This is done
// by the RPC server after the subscription ID is send to the client.
func (n *Notifier) CreateSubscription() *Subscription {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.sub != nil {
		panic("can't create multiple subscriptions with Notifier")
	} else if n.callReturned {
		panic("can't create subscription after subscribe call has returned")
	}
	n.sub = &Subscription{ID: n.h.idgen(), namespace: n.namespace, err: make(chan error, 1)}
	return n.sub
}

// Notify sends a notification to the client with the given data as payload.
// If an error occurs the RPC connection is closed and the error is returned.
//
// Once a previous Notify (or activate) has seen a write failure, this method
// returns errSubscriptionClosed immediately without re-attempting the write,
// so a slow or wedged client can't keep the per-subscription goroutine
// blocked on the 10-second writeJSON deadline for every queued event.
func (n *Notifier) Notify(id ID, data any) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.sub == nil {
		panic("can't Notify before subscription is created")
	} else if n.sub.ID != id {
		panic("Notify with wrong ID")
	}
	if n.failed {
		return errSubscriptionClosed
	}
	if n.activated {
		return n.send(n.sub, data)
	}
	n.buffer = append(n.buffer, data)
	return nil
}

// takeSubscription returns the subscription (if one has been created). No subscription can
// be created after this call.
func (n *Notifier) takeSubscription() *Subscription {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.callReturned = true
	return n.sub
}

// activate is called after the subscription ID was sent to client. Notifications are
// buffered before activation. This prevents notifications being sent to the client before
// the subscription ID is sent to the client.
func (n *Notifier) activate() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	for _, data := range n.buffer {
		if err := n.send(n.sub, data); err != nil {
			return err
		}
	}
	n.activated = true
	return nil
}

func (n *Notifier) send(sub *Subscription, data any) error {
	params, err := json.Marshal(subscriptionResultEnc{
		ID:     string(sub.ID),
		Result: data,
	})
	if err != nil {
		return err
	}
	msg := jsonrpcMessage{
		Version: vsn,
		Method:  n.namespace + notificationMethodSuffix,
		Params:  params,
	}
	err = n.h.conn.writeJSON(context.Background(), &msg, false)
	if err != nil {
		// Write failed (deadline exceeded, encode error, broken
		// connection). Treat the notifier as dead so future Notify
		// calls short-circuit, signal the subscription's err channel
		// so any consumer goroutine exits, and close the underlying
		// codec so the WS / IPC server-side cleans up. This protects
		// the producer (typically a per-subscription goroutine in a
		// service like txtracker) from blocking 10s per queued event
		// against a wedged client.
		//
		// Holding n.mu here is fine: sub.close is sync.Once-guarded
		// and codec.close is sync.Once-guarded; neither re-enters the
		// notifier.
		n.failed = true
		sub.close(err)
		if codec, ok := n.h.conn.(ServerCodec); ok {
			codec.close()
		}
	}
	return err
}

// A Subscription is created by a notifier and tied to that notifier. The client can use
// this subscription to wait for an unsubscribe request for the client, see Err().
type Subscription struct {
	ID        ID
	namespace string
	err       chan error // closed on unsubscribe
	closeOnce sync.Once  // guards close(err) — idempotent across paths
}

// Err returns a channel that is closed when the client send an unsubscribe request.
func (s *Subscription) Err() <-chan error {
	return s.err
}

// close signals the subscription as terminated and closes its err
// channel. Idempotent — multiple paths can race to close (a write
// failure inside Notifier.send and the handler's normal teardown via
// cancelServerSubscriptions); only the first call has any effect.
//
// If err is non-nil it is delivered on the err channel before close
// (best-effort; the channel is buffered so this never blocks but is
// guarded by the default branch in case the buffer was already used).
func (s *Subscription) close(err error) {
	s.closeOnce.Do(func() {
		if err != nil {
			select {
			case s.err <- err:
			default:
			}
		}
		close(s.err)
	})
}

// MarshalJSON marshals a subscription as its ID.
func (s *Subscription) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.ID)
}

// ClientSubscription is a subscription established through the Client's Subscribe or
// EthSubscribe methods.
type ClientSubscription struct {
	client    *Client
	etype     reflect.Type
	channel   reflect.Value
	namespace string
	subid     string

	// The in channel receives notification values from client dispatcher.
	in chan json.RawMessage

	// The error channel receives the error from the forwarding loop.
	// It is closed by Unsubscribe.
	err     chan error
	errOnce sync.Once

	// Closing of the subscription is requested by sending on 'quit'. This is handled by
	// the forwarding loop, which closes 'forwardDone' when it has stopped sending to
	// sub.channel. Finally, 'unsubDone' is closed after unsubscribing on the server side.
	quit        chan error
	forwardDone chan struct{}
	unsubDone   chan struct{}
}

// This is the sentinel value sent on sub.quit when Unsubscribe is called.
var errUnsubscribed = errors.New("unsubscribed")

func newClientSubscription(c *Client, namespace string, channel reflect.Value) *ClientSubscription {
	sub := &ClientSubscription{
		client:      c,
		namespace:   namespace,
		etype:       channel.Type().Elem(),
		channel:     channel,
		in:          make(chan json.RawMessage),
		quit:        make(chan error),
		forwardDone: make(chan struct{}),
		unsubDone:   make(chan struct{}),
		err:         make(chan error, 1),
	}
	return sub
}

// Err returns the subscription error channel. The intended use of Err is to schedule
// resubscription when the client connection is closed unexpectedly.
//
// The error channel receives a value when the subscription has ended due to an error. The
// received error is nil if Close has been called on the underlying client and no other
// error has occurred.
//
// The error channel is closed when Unsubscribe is called on the subscription.
func (sub *ClientSubscription) Err() <-chan error {
	return sub.err
}

// Unsubscribe unsubscribes the notification and closes the error channel.
// It can safely be called more than once.
func (sub *ClientSubscription) Unsubscribe() {
	sub.errOnce.Do(func() {
		select {
		case sub.quit <- errUnsubscribed:
			<-sub.unsubDone
		case <-sub.unsubDone:
		}
		close(sub.err)
	})
}

// deliver is called by the client's message dispatcher to send a notification value.
func (sub *ClientSubscription) deliver(result json.RawMessage) (ok bool) {
	select {
	case sub.in <- result:
		return true
	case <-sub.forwardDone:
		return false
	}
}

// close is called by the client's message dispatcher when the connection is closed.
func (sub *ClientSubscription) close(err error) {
	select {
	case sub.quit <- err:
	case <-sub.forwardDone:
	}
}

// run is the forwarding loop of the subscription. It runs in its own goroutine and
// is launched by the client's handler after the subscription has been created.
func (sub *ClientSubscription) run() {
	defer close(sub.unsubDone)

	unsubscribe, err := sub.forward()

	// The client's dispatch loop won't be able to execute the unsubscribe call if it is
	// blocked in sub.deliver() or sub.close(). Closing forwardDone unblocks them.
	close(sub.forwardDone)

	// Call the unsubscribe method on the server.
	if unsubscribe {
		sub.requestUnsubscribe()
	}

	// Send the error.
	if err != nil {
		if err == ErrClientQuit {
			// ErrClientQuit gets here when Client.Close is called. This is reported as a
			// nil error because it's not an error, but we can't close sub.err here.
			err = nil
		}
		sub.err <- err
	}
}

// forward is the forwarding loop. It takes in RPC notifications and sends them
// on the subscription channel.
func (sub *ClientSubscription) forward() (unsubscribeServer bool, err error) {
	cases := []reflect.SelectCase{
		{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(sub.quit)},
		{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(sub.in)},
		{Dir: reflect.SelectSend, Chan: sub.channel},
	}
	buffer := list.New()

	for {
		var chosen int
		var recv reflect.Value
		if buffer.Len() == 0 {
			// Idle, omit send case.
			chosen, recv, _ = reflect.Select(cases[:2])
		} else {
			// Non-empty buffer, send the first queued item.
			cases[2].Send = reflect.ValueOf(buffer.Front().Value)
			chosen, recv, _ = reflect.Select(cases)
		}

		switch chosen {
		case 0: // <-sub.quit
			if !recv.IsNil() {
				err = recv.Interface().(error)
			}
			if err == errUnsubscribed {
				// Exiting because Unsubscribe was called, unsubscribe on server.
				return true, nil
			}
			return false, err

		case 1: // <-sub.in
			val, err := sub.unmarshal(recv.Interface().(json.RawMessage))
			if err != nil {
				return true, err
			}
			if buffer.Len() == maxClientSubscriptionBuffer {
				return true, ErrSubscriptionQueueOverflow
			}
			buffer.PushBack(val)

		case 2: // sub.channel<-
			cases[2].Send = reflect.Value{} // Don't hold onto the value.
			buffer.Remove(buffer.Front())
		}
	}
}

func (sub *ClientSubscription) unmarshal(result json.RawMessage) (interface{}, error) {
	val := reflect.New(sub.etype)
	err := json.Unmarshal(result, val.Interface())
	return val.Elem().Interface(), err
}

func (sub *ClientSubscription) requestUnsubscribe() error {
	var result interface{}
	ctx, cancel := context.WithTimeout(context.Background(), unsubscribeTimeout)
	defer cancel()
	err := sub.client.CallContext(ctx, &result, sub.namespace+unsubscribeMethodSuffix, sub.subid)
	return err
}
