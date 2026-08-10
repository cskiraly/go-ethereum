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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

func TestNewID(t *testing.T) {
	t.Parallel()

	hexchars := "0123456789ABCDEFabcdef"
	for i := 0; i < 100; i++ {
		id := string(NewID())
		if !strings.HasPrefix(id, "0x") {
			t.Fatalf("invalid ID prefix, want '0x...', got %s", id)
		}

		id = id[2:]
		if len(id) == 0 || len(id) > 32 {
			t.Fatalf("invalid ID length, want len(id) > 0 && len(id) <= 32), got %d", len(id))
		}

		for i := 0; i < len(id); i++ {
			if strings.IndexByte(hexchars, id[i]) == -1 {
				t.Fatalf("unexpected byte, want any valid hex char, got %c", id[i])
			}
		}
	}
}

func TestSubscriptions(t *testing.T) {
	t.Parallel()

	var (
		namespaces        = []string{"eth"}
		service           = &notificationTestService{}
		subCount          = len(namespaces)
		notificationCount = 3

		server                 = NewServer()
		clientConn, serverConn = net.Pipe()
		out                    = json.NewEncoder(clientConn)
		in                     = json.NewDecoder(clientConn)
		successes              = make(chan subConfirmation)
		notifications          = make(chan subscriptionResult)
		errors                 = make(chan error, subCount*notificationCount+1)
	)

	// setup and start server
	for _, namespace := range namespaces {
		if err := server.RegisterName(namespace, service); err != nil {
			t.Fatalf("unable to register test service %v", err)
		}
	}
	go server.ServeCodec(NewCodec(serverConn), 0)
	defer server.Stop()

	// wait for message and write them to the given channels
	go waitForMessages(in, successes, notifications, errors)

	// create subscriptions one by one
	for i, namespace := range namespaces {
		request := map[string]interface{}{
			"id":      i,
			"method":  fmt.Sprintf("%s_subscribe", namespace),
			"jsonrpc": "2.0",
			"params":  []interface{}{"someSubscription", notificationCount, i},
		}
		if err := out.Encode(&request); err != nil {
			t.Fatalf("Could not create subscription: %v", err)
		}
	}

	timeout := time.After(30 * time.Second)
	subids := make(map[string]string, subCount)
	count := make(map[string]int, subCount)
	allReceived := func() bool {
		done := len(count) == subCount
		for _, c := range count {
			if c < notificationCount {
				done = false
			}
		}
		return done
	}
	for !allReceived() {
		select {
		case confirmation := <-successes: // subscription created
			subids[namespaces[confirmation.reqid]] = string(confirmation.subid)
		case notification := <-notifications:
			count[notification.ID]++
		case err := <-errors:
			t.Fatal(err)
		case <-timeout:
			for _, namespace := range namespaces {
				subid, found := subids[namespace]
				if !found {
					t.Errorf("subscription for %q not created", namespace)
					continue
				}
				if count, found := count[subid]; !found || count < notificationCount {
					t.Errorf("didn't receive all notifications (%d<%d) in time for namespace %q", count, notificationCount, namespace)
				}
			}
			t.Fatal("timed out")
		}
	}
}

// This test checks that unsubscribing works.
func TestServerUnsubscribe(t *testing.T) {
	t.Parallel()

	p1, p2 := net.Pipe()
	defer p2.Close()

	// Start the server.
	server := newTestServer()
	service := &notificationTestService{unsubscribed: make(chan string, 1)}
	server.RegisterName("nftest2", service)
	go server.ServeCodec(NewCodec(p1), 0)

	// Subscribe.
	p2.SetDeadline(time.Now().Add(10 * time.Second))
	p2.Write([]byte(`{"jsonrpc":"2.0","id":1,"method":"nftest2_subscribe","params":["someSubscription",0,10]}`))

	// Handle received messages.
	var (
		resps         = make(chan subConfirmation)
		notifications = make(chan subscriptionResult)
		errors        = make(chan error, 1)
	)
	go waitForMessages(json.NewDecoder(p2), resps, notifications, errors)

	// Receive the subscription ID.
	var sub subConfirmation
	select {
	case sub = <-resps:
	case err := <-errors:
		t.Fatal(err)
	}

	// Unsubscribe and check that it is handled on the server side.
	p2.Write([]byte(`{"jsonrpc":"2.0","method":"nftest2_unsubscribe","params":["` + sub.subid + `"]}`))
	for {
		select {
		case id := <-service.unsubscribed:
			if id != string(sub.subid) {
				t.Errorf("wrong subscription ID unsubscribed")
			}
			return
		case err := <-errors:
			t.Fatal(err)
		case <-notifications:
			// drop notifications
		}
	}
}

type subConfirmation struct {
	reqid int
	subid ID
}

// waitForMessages reads RPC messages from 'in' and dispatches them into the given channels.
// It stops if there is an error.
func waitForMessages(in *json.Decoder, successes chan subConfirmation, notifications chan subscriptionResult, errors chan error) {
	for {
		resp, notification, err := readAndValidateMessage(in)
		if err != nil {
			errors <- err
			return
		} else if resp != nil {
			successes <- *resp
		} else {
			notifications <- *notification
		}
	}
}

func readAndValidateMessage(in *json.Decoder) (*subConfirmation, *subscriptionResult, error) {
	var msg jsonrpcMessage
	if err := in.Decode(&msg); err != nil {
		return nil, nil, fmt.Errorf("decode error: %v", err)
	}
	switch {
	case msg.isNotification():
		var res subscriptionResult
		if err := json.Unmarshal(msg.Params, &res); err != nil {
			return nil, nil, fmt.Errorf("invalid subscription result: %v", err)
		}
		return nil, &res, nil
	case msg.isResponse():
		var c subConfirmation
		if msg.Error != nil {
			return nil, nil, msg.decodeError()
		} else if err := json.Unmarshal(msg.Result, &c.subid); err != nil {
			return nil, nil, fmt.Errorf("invalid response: %v", err)
		} else {
			json.Unmarshal(msg.ID, &c.reqid)
			return &c, nil, nil
		}
	default:
		return nil, nil, fmt.Errorf("unrecognized message: %v", msg)
	}
}

type mockConn struct {
	w io.Writer
}

func (c *mockConn) writeJSON(ctx context.Context, msg *jsonrpcMessage, isError bool) error {
	buf := appendMessage(nil, msg)
	buf = append(buf, '\n')
	_, err := c.w.Write(buf)
	return err
}

func (c *mockConn) writeJSONBatch(ctx context.Context, msgs []*jsonrpcMessage, isError bool) error {
	buf := appendBatch(nil, msgs)
	buf = append(buf, '\n')
	_, err := c.w.Write(buf)
	return err
}

// closed returns a channel which is closed when the connection is closed.
func (c *mockConn) closed() <-chan interface{} { return nil }

// remoteAddr returns the peer address of the connection.
func (c *mockConn) remoteAddr() string { return "" }

// BenchmarkNotify benchmarks the performance of notifying a subscription.
func BenchmarkNotify(b *testing.B) {
	id := ID("test")
	notifier := &Notifier{
		h:         &handler{conn: &mockConn{io.Discard}},
		sub:       &Subscription{ID: id},
		activated: true,
	}
	msg := &types.Header{
		ParentHash: common.HexToHash("0x01"),
		Number:     big.NewInt(100),
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		notifier.Notify(id, msg)
	}
}

func TestNotify(t *testing.T) {
	t.Parallel()

	out := new(bytes.Buffer)
	id := ID("test")
	notifier := &Notifier{
		h:         &handler{conn: &mockConn{out}},
		sub:       &Subscription{ID: id},
		activated: true,
	}
	notifier.Notify(id, "hello")
	have := strings.TrimSpace(out.String())
	want := `{"jsonrpc":"2.0","method":"_subscription","params":{"subscription":"test","result":"hello"}}`
	if have != want {
		t.Errorf("have:\n%v\nwant:\n%v\n", have, want)
	}
}

// failingConn is a ServerCodec whose writeJSON always errors. close
// is sync.Once-guarded and observable via wasClosed() so tests can
// assert the codec teardown actually fired.
type failingConn struct {
	closeMu sync.Mutex
	closed_ bool
}

func (c *failingConn) writeJSON(ctx context.Context, msg *jsonrpcMessage, isError bool) error {
	return errors.New("boom: simulated wedged client")
}
func (c *failingConn) writeJSONBatch(ctx context.Context, msgs []*jsonrpcMessage, isError bool) error {
	return errors.New("boom: simulated wedged client")
}
func (c *failingConn) closed() <-chan interface{} { return nil }
func (c *failingConn) remoteAddr() string         { return "" }
func (c *failingConn) peerInfo() PeerInfo         { return PeerInfo{} }
func (c *failingConn) readBatch() (msgs []*jsonrpcMessage, isBatch bool, err error) {
	return nil, false, io.EOF
}
func (c *failingConn) close() {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	c.closed_ = true
}
func (c *failingConn) wasClosed() bool {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	return c.closed_
}

// TestNotifyWriteFailureClosesSubscription verifies that a Notify
// call whose write fails causes the notifier to mark itself failed,
// close the subscription's err channel, and tear down the underlying
// codec — protecting geth from a wedged client that would otherwise
// keep the per-subscription goroutine bouncing off the writeJSON
// deadline for every queued event.
func TestNotifyWriteFailureClosesSubscription(t *testing.T) {
	t.Parallel()

	conn := &failingConn{}
	id := ID("test")
	sub := &Subscription{ID: id, err: make(chan error, 1)}
	notifier := &Notifier{
		h:         &handler{conn: conn},
		sub:       sub,
		activated: true,
	}

	// First Notify hits the failing writer and trips the protection.
	if err := notifier.Notify(id, "first"); err == nil {
		t.Fatal("expected error from first Notify, got nil")
	}

	// sub.err is closed (with the original write error delivered).
	select {
	case err, ok := <-sub.Err():
		if !ok {
			t.Fatal("sub.err closed without delivering the write error")
		}
		if err == nil || !strings.Contains(err.Error(), "boom") {
			t.Errorf("unexpected error on sub.err: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("sub.err didn't fire after write failure")
	}

	// Codec was closed so the connection-shutdown path engages.
	if !conn.wasClosed() {
		t.Error("codec.close was not invoked after write failure")
	}

	// Subsequent Notify short-circuits with errSubscriptionClosed —
	// no second write attempt to the wedged conn.
	if err := notifier.Notify(id, "second"); !errors.Is(err, errSubscriptionClosed) {
		t.Errorf("second Notify: got %v, want errSubscriptionClosed", err)
	}
}

// TestSubscriptionDoubleCloseIsIdempotent verifies that the
// sync.Once guard on Subscription.close prevents the panic that
// would otherwise occur when both the write-failure path and
// cancelServerSubscriptions race to close the same sub.
func TestSubscriptionDoubleCloseIsIdempotent(t *testing.T) {
	t.Parallel()

	sub := &Subscription{err: make(chan error, 1)}
	sub.close(errors.New("first"))
	sub.close(errors.New("second")) // would panic without the sync.Once

	// First error delivered; channel closed.
	if err, ok := <-sub.Err(); !ok || err == nil {
		t.Errorf("expected first error on sub.err, got err=%v ok=%v", err, ok)
	}
	// Second drain returns zero value because channel is closed.
	if err, ok := <-sub.Err(); ok {
		t.Errorf("expected sub.err to be closed, got err=%v", err)
	}
}
