// Copyright 2026 The go-ethereum Authors
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

package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/donovanhide/eventsource"
	"github.com/ethereum/go-ethereum/beacon/types"
	"github.com/ethereum/go-ethereum/common"
)

// TestHeadListenerResubscribe checks that the head listener subscribes to the event
// stream again after it ends, and that failed attempts don't make it wait longer and
// longer.
func TestHeadListenerResubscribe(t *testing.T) {
	defer func(oldMin, oldMax time.Duration) {
		minEventStreamWait, maxEventStreamWait = oldMin, oldMax
	}(minEventStreamWait, maxEventStreamWait)
	minEventStreamWait, maxEventStreamWait = time.Millisecond, 5*time.Millisecond

	// The first stream sends a head and ends, the next subscriptions fail, then a
	// stream sends the next head. A wait doubling without a cap would reach 2^20 ms
	// (17 minutes) before the last subscription.
	const failures = 20
	var subscriptions atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/eth/v1/events" {
			http.NotFound(w, r)
			return
		}
		n := subscriptions.Add(1)
		if n > 1 && n <= 1+failures {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintf(w, "event: head\ndata: {\"slot\":\"%d\",\"block\":\"%s\"}\n\n", n, common.Hash{byte(n)}.Hex())
		w.(http.Flusher).Flush()
		if n > 1 {
			<-r.Context().Done() // keep the stream open until the listener stops
		}
	}))
	defer server.Close()

	var (
		heads = make(chan uint64, 10)
		errs  atomic.Int32
	)
	stop := NewBeaconLightApi(server.URL, nil).StartHeadListener(HeadEventListener{
		OnNewHead:    func(slot uint64, blockRoot common.Hash) { heads <- slot },
		OnOptimistic: func(types.OptimisticUpdate) {},
		OnFinality:   func(types.FinalityUpdate) {},
		OnError:      func(error) { errs.Add(1) },
	})
	defer stop()

	for _, want := range []uint64{1, failures + 2} {
		select {
		case slot := <-heads:
			if slot != want {
				t.Fatalf("wrong head slot: got %d, want %d", slot, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("no head event for slot %d", want)
		}
	}
	// The end of the first stream and every failed subscription are reported.
	if n := errs.Load(); n != failures+1 {
		t.Fatalf("wrong number of errors: got %d, want %d", n, failures+1)
	}
}

// TestEventStreamWait checks the waits between subscriptions to the event stream:
// doubling after attempts without events, up to the cap, and back to the start
// after an attempt with events.
func TestEventStreamWait(t *testing.T) {
	var (
		received = []bool{false, false, false, false, false, false, false, true, false}
		want     = []time.Duration{1, 2, 4, 8, 16, 30, 30, 1, 2}
		wait     time.Duration
	)
	for i := range received {
		wait = eventStreamWait(wait, received[i])
		if wait != want[i]*time.Second {
			t.Fatalf("wait %d: got %v, want %v", i, wait, want[i]*time.Second)
		}
	}
}

// TestReadEventStreamReceived checks that only named events count as received:
// other text, such as a web page, doesn't reset the wait between subscriptions.
func TestReadEventStreamReceived(t *testing.T) {
	for _, tc := range []struct {
		body     string
		received bool
	}{
		{"<html>\n<body>not an event stream</body>\n</html>\n\n", false},
		{"event: head\ndata: {}\n\n", true},
	} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, tc.body)
		}))
		received, err := NewBeaconLightApi(server.URL, nil).readEventStream(context.Background(), make(chan eventsource.Event, 10))
		server.Close()
		if received != tc.received || err == nil {
			t.Errorf("body %q: received %v, error %v; want received %v and the end of the stream", tc.body, received, err, tc.received)
		}
	}
}

// TestHeadListenerStop checks that stopping the head listener returns while a
// subscription waits for the response, and while a stream is open.
func TestHeadListenerStop(t *testing.T) {
	for _, open := range []bool{false, true} {
		t.Run(fmt.Sprintf("open=%v", open), func(t *testing.T) {
			var (
				subscribed = make(chan struct{}, 1)
				release    = make(chan struct{})
			)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/eth/v1/events" {
					http.NotFound(w, r)
					return
				}
				if open {
					w.Header().Set("Content-Type", "text/event-stream")
					w.(http.Flusher).Flush()
				}
				select {
				case subscribed <- struct{}{}:
				default:
				}
				select {
				case <-r.Context().Done():
				case <-release:
				}
			}))
			defer server.Close()
			defer close(release)

			stop := NewBeaconLightApi(server.URL, nil).StartHeadListener(HeadEventListener{
				OnNewHead:    func(uint64, common.Hash) {},
				OnOptimistic: func(types.OptimisticUpdate) {},
				OnFinality:   func(types.FinalityUpdate) {},
				OnError:      func(error) {},
			})
			select {
			case <-subscribed:
			case <-time.After(5 * time.Second):
				t.Fatal("no subscription")
			}
			stopped := make(chan struct{})
			go func() {
				stop()
				close(stopped)
			}()
			select {
			case <-stopped:
			case <-time.After(5 * time.Second):
				t.Fatal("the listener didn't stop")
			}
		})
	}
}
