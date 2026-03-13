// Copyright 2026 The go-ethereum Authors
// This file is part of go-ethereum.
//
// go-ethereum is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// go-ethereum is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with go-ethereum. If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/chromedp/chromedp"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rpc"
)

// ========================================================================
// Mock txtracker RPC service
// ========================================================================

// mockEvent mirrors the JSON structure that app.js expects from subscription
// notifications. We use a simple map so we don't import the txtracker package.
type mockEvent struct {
	TxHash    common.Hash `json:"txHash"`
	OldStatus string      `json:"oldStatus"`
	NewStatus string      `json:"newStatus"`
	Peer      string      `json:"peer,omitempty"`
	BlockNum  uint64      `json:"blockNum,omitempty"`
	RejectErr string      `json:"rejectErr,omitempty"`
	TxType    uint8       `json:"txType"`
}

// mockTxtrackerService implements the RPC methods that app.js calls.
type mockTxtrackerService struct {
	mu           sync.Mutex
	eventCh      chan mockEvent     // test injects events here
	subscribedCh chan struct{}       // closed when browser subscribes
	subscribed   bool
}

func newMockService() *mockTxtrackerService {
	return &mockTxtrackerService{
		eventCh:      make(chan mockEvent, 256),
		subscribedCh: make(chan struct{}),
	}
}

// Events is called by the RPC framework when the browser calls
// txtracker_subscribe("events").
func (s *mockTxtrackerService) Events(ctx context.Context) (*rpc.Subscription, error) {
	notifier, supported := rpc.NotifierFromContext(ctx)
	if !supported {
		return nil, rpc.ErrNotificationsUnsupported
	}

	sub := notifier.CreateSubscription()

	// Signal that the subscription is ready.
	s.mu.Lock()
	if !s.subscribed {
		s.subscribed = true
		close(s.subscribedCh)
	}
	s.mu.Unlock()

	go func() {
		for {
			select {
			case ev := <-s.eventCh:
				if err := notifier.Notify(sub.ID, ev); err != nil {
					return
				}
			case <-sub.Err():
				return
			}
		}
	}()

	return sub, nil
}

// GetTx returns a canned response (app.js calls txtracker_getTx).
func (s *mockTxtrackerService) GetTx(hash common.Hash) map[string]interface{} {
	return map[string]interface{}{
		"Hash":   hash.Hex(),
		"Status": "announced",
	}
}

// GetAllPeerStats returns an empty map (app.js calls txtracker_getAllPeerStats).
func (s *mockTxtrackerService) GetAllPeerStats() map[string]interface{} {
	return map[string]interface{}{}
}

// GetStats returns canned eviction stats (app.js calls txtracker_getStats).
func (s *mockTxtrackerService) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"Capacity": 65536,
		"Size":     0,
	}
}

// ========================================================================
// Test infrastructure
// ========================================================================

// startMockGeth starts a mock RPC server that the txview proxy can connect to.
// Returns the WebSocket URL, the mock service (for event injection), and cleanup.
func startMockGeth(t *testing.T) (wsURL string, svc *mockTxtrackerService) {
	t.Helper()

	svc = newMockService()
	rpcSrv := rpc.NewServer()
	if err := rpcSrv.RegisterName("txtracker", svc); err != nil {
		t.Fatalf("failed to register mock service: %v", err)
	}

	httpSrv := httptest.NewServer(rpcSrv.WebsocketHandler([]string{"*"}))
	t.Cleanup(func() {
		httpSrv.Close()
		rpcSrv.Stop()
	})

	wsURL = "ws:" + strings.TrimPrefix(httpSrv.URL, "http:")
	return wsURL, svc
}

// startTxview starts a txview HTTP server backed by the given mock geth WS URL.
// Returns the HTTP URL of the txview server.
func startTxview(t *testing.T, gethWS string) string {
	t.Helper()

	// Convert ws:// to http:// for the proxy target.
	httpEndpoint := strings.Replace(strings.Replace(gethWS, "ws://", "http://", 1), "wss://", "https://", 1)
	target, err := url.Parse(httpEndpoint)
	if err != nil {
		t.Fatalf("failed to parse mock geth URL: %v", err)
	}

	mux, err := setupMux(target)
	if err != nil {
		t.Fatalf("setupMux failed: %v", err)
	}

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv.URL
}

// findChromium returns the path to a Chrome/Chromium binary, or empty string.
func findChromium() string {
	for _, name := range []string{"chromium-browser", "chromium", "google-chrome", "google-chrome-stable"} {
		if p, err := exec.LookPath(name); err == nil {
			return p
		}
	}
	return ""
}

// newBrowser creates a chromedp context with a headless browser at 1280x800.
// Skips the test if no Chrome/Chromium is available.
func newBrowser(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()

	chromePath := findChromium()
	if chromePath == "" {
		t.Skip("no Chrome/Chromium binary found, skipping browser test")
	}

	opts := append(chromedp.DefaultExecAllocatorOptions[:],
		chromedp.ExecPath(chromePath),
		chromedp.Flag("headless", true),
		chromedp.Flag("disable-gpu", true),
		chromedp.Flag("no-sandbox", true),
		chromedp.WindowSize(1280, 800),
	)

	allocCtx, allocCancel := chromedp.NewExecAllocator(context.Background(), opts...)
	ctx, cancel := chromedp.NewContext(allocCtx)

	t.Cleanup(func() {
		cancel()
		allocCancel()
	})

	return ctx, cancel
}

// waitSubscribed waits for the browser to complete txtracker_subscribe.
func waitSubscribed(t *testing.T, svc *mockTxtrackerService) {
	t.Helper()
	select {
	case <-svc.subscribedCh:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for browser to subscribe")
	}
}

// injectEvents sends n unique events with sequential hashes.
func injectEvents(svc *mockTxtrackerService, start, n int, status string) {
	for i := start; i < start+n; i++ {
		hash := common.Hash{}
		hash[0] = byte(i >> 8)
		hash[1] = byte(i)
		svc.eventCh <- mockEvent{
			TxHash:    hash,
			NewStatus: status,
			Peer:      "peer1",
			TxType:    2,
		}
	}
}

// injectStatusUpdate sends a status update for an existing hash.
func injectStatusUpdate(svc *mockTxtrackerService, hashIdx int, status string) {
	hash := common.Hash{}
	hash[0] = byte(hashIdx >> 8)
	hash[1] = byte(hashIdx)
	svc.eventCh <- mockEvent{
		TxHash:    hash,
		NewStatus: status,
		Peer:      "peer1",
		TxType:    2,
	}
}

// ========================================================================
// Lightweight endpoint tests (no browser)
// ========================================================================

func TestConfigEndpoint(t *testing.T) {
	gethWS, _ := startMockGeth(t)
	txviewURL := startTxview(t, gethWS)

	resp, err := http.Get(txviewURL + "/config")
	if err != nil {
		t.Fatalf("GET /config failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Fatalf("expected application/json, got %q", ct)
	}

	var cfg map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&cfg); err != nil {
		t.Fatalf("failed to decode JSON: %v", err)
	}
	ep, ok := cfg["rpcEndpoint"]
	if !ok {
		t.Fatal("missing rpcEndpoint in response")
	}
	if !strings.HasPrefix(ep, "ws://") || !strings.HasSuffix(ep, "/ws") {
		t.Fatalf("unexpected rpcEndpoint: %q", ep)
	}
}

func TestConfigEndpointTLS(t *testing.T) {
	// Verify that the /config endpoint returns wss:// when TLS is set.
	// We can't easily test real TLS with httptest, so we directly test
	// the handler with a request that has TLS set.
	target, _ := url.Parse("http://localhost:8546")
	mux, err := setupMux(target)
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest("GET", "/config", nil)
	req.TLS = &tls.ConnectionState{} // simulate HTTPS
	req.Host = "example.com"
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	var cfg map[string]string
	if err := json.NewDecoder(w.Body).Decode(&cfg); err != nil {
		t.Fatalf("failed to decode JSON: %v", err)
	}
	if !strings.HasPrefix(cfg["rpcEndpoint"], "wss://") {
		t.Fatalf("expected wss:// scheme for TLS, got %q", cfg["rpcEndpoint"])
	}
}

func TestStaticAssets(t *testing.T) {
	gethWS, _ := startMockGeth(t)
	txviewURL := startTxview(t, gethWS)

	tests := []struct {
		path        string
		wantStatus  int
		wantCTSub   string // substring of Content-Type
		wantBodySub string // substring of body
	}{
		{"/", 200, "text/html", "<title>txview"},
		{"/app.js", 200, "javascript", "txview"},
		{"/helpers.js", 200, "javascript", "txviewHelpers"},
		{"/style.css", 200, "css", "body"},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			resp, err := http.Get(txviewURL + tt.path)
			if err != nil {
				t.Fatalf("GET %s failed: %v", tt.path, err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != tt.wantStatus {
				t.Fatalf("expected %d, got %d", tt.wantStatus, resp.StatusCode)
			}
			if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, tt.wantCTSub) {
				t.Fatalf("expected Content-Type containing %q, got %q", tt.wantCTSub, ct)
			}
		})
	}
}

func TestStaticAssets404(t *testing.T) {
	gethWS, _ := startMockGeth(t)
	txviewURL := startTxview(t, gethWS)

	resp, err := http.Get(txviewURL + "/nonexistent.txt")
	if err != nil {
		t.Fatalf("GET /nonexistent.txt failed: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 404 {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}

func TestTxDetailPage(t *testing.T) {
	gethWS, _ := startMockGeth(t)
	txviewURL := startTxview(t, gethWS)

	resp, err := http.Get(txviewURL + "/tx/0xabc123")
	if err != nil {
		t.Fatalf("GET /tx/0xabc123 failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/html") {
		t.Fatalf("expected text/html, got %q", ct)
	}
}

// ========================================================================
// WebSocket proxy test (no browser, verifies RPC subscription works)
// ========================================================================

func TestWSProxySubscription(t *testing.T) {
	gethWS, svc := startMockGeth(t)
	txviewURL := startTxview(t, gethWS)

	// Dial the txview /ws proxy endpoint.
	wsURL := "ws:" + strings.TrimPrefix(txviewURL, "http:") + "/ws"
	client, err := rpc.DialWebsocket(context.Background(), wsURL, "")
	if err != nil {
		t.Fatalf("failed to dial txview /ws: %v", err)
	}
	defer client.Close()

	// Subscribe to txtracker events through the proxy.
	eventCh := make(chan json.RawMessage, 16)
	sub, err := client.Subscribe(context.Background(), "txtracker", eventCh, "events")
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	// Wait for the mock service to see the subscription.
	waitSubscribed(t, svc)

	// Inject a test event.
	testHash := common.HexToHash("0xdeadbeef")
	svc.eventCh <- mockEvent{
		TxHash:    testHash,
		NewStatus: "announced",
		Peer:      "testpeer",
	}

	// Read the notification.
	select {
	case raw := <-eventCh:
		var ev mockEvent
		if err := json.Unmarshal(raw, &ev); err != nil {
			t.Fatalf("failed to unmarshal event: %v", err)
		}
		if ev.TxHash != testHash {
			t.Fatalf("expected hash %s, got %s", testHash.Hex(), ev.TxHash.Hex())
		}
		if ev.NewStatus != "announced" {
			t.Fatalf("expected status announced, got %s", ev.NewStatus)
		}
	case err := <-sub.Err():
		t.Fatalf("subscription error: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for event notification")
	}
}

// ========================================================================
// Browser E2E tests (chromedp)
// ========================================================================

func TestFeedScrollPause(t *testing.T) {
	gethWS, svc := startMockGeth(t)
	txviewURL := startTxview(t, gethWS)
	ctx, _ := newBrowser(t)

	// Navigate and wait for subscription.
	if err := chromedp.Run(ctx, chromedp.Navigate(txviewURL)); err != nil {
		t.Fatalf("navigate failed: %v", err)
	}
	waitSubscribed(t, svc)

	// Inject 30 events to populate the feed (enough to make it scrollable).
	injectEvents(svc, 1, 30, "announced")

	// Wait for rows to render and spacer to exceed viewport.
	if err := chromedp.Run(ctx,
		chromedp.Poll(`
			(function() {
				var vp = document.getElementById('viewport');
				var sp = document.getElementById('scroll-spacer');
				return sp && vp && parseInt(sp.style.height) > vp.clientHeight;
			})()
		`, nil, chromedp.WithPollingInterval(100*time.Millisecond)),
	); err != nil {
		t.Fatalf("waiting for scrollable feed: %v", err)
	}

	// Scroll viewport down by 200px.
	if err := chromedp.Run(ctx,
		chromedp.Evaluate(`document.getElementById('viewport').scrollTop = 200`, nil),
		chromedp.Sleep(500*time.Millisecond), // let scroll handler fire
	); err != nil {
		t.Fatalf("scroll failed: %v", err)
	}

	// Record anchor: the first visible row's hash.
	var anchorHash string
	if err := chromedp.Run(ctx,
		chromedp.Evaluate(`
			(function() {
				var rows = document.getElementById('row-container').children;
				if (rows.length > 0) return rows[0].dataset.hash;
				return '';
			})()
		`, &anchorHash),
	); err != nil {
		t.Fatalf("failed to get anchor hash: %v", err)
	}
	if anchorHash == "" {
		t.Fatal("no anchor row found after scrolling")
	}

	// Inject 5 new events while paused.
	injectEvents(svc, 100, 5, "announced")

	// Wait for pill to appear.
	if err := chromedp.Run(ctx,
		chromedp.Poll(`
			(function() {
				var pill = document.querySelector('.feed-new-pill');
				return pill && pill.style.display !== 'none' && pill.textContent.includes('new');
			})()
		`, nil, chromedp.WithPollingInterval(100*time.Millisecond)),
	); err != nil {
		t.Fatalf("pill did not appear: %v", err)
	}

	// Verify pill text contains "5 new".
	var pillText string
	if err := chromedp.Run(ctx,
		chromedp.Evaluate(`document.querySelector('.feed-new-pill').textContent`, &pillText),
	); err != nil {
		t.Fatalf("failed to get pill text: %v", err)
	}
	if !strings.Contains(pillText, "5 new") {
		t.Fatalf("expected pill to contain '5 new', got %q", pillText)
	}

	// Verify anchor row is still visible (not scrolled to top).
	var anchorStillVisible bool
	if err := chromedp.Run(ctx,
		chromedp.Evaluate(fmt.Sprintf(`
			(function() {
				var rows = document.getElementById('row-container').children;
				for (var i = 0; i < rows.length; i++) {
					if (rows[i].dataset.hash === %q) return true;
				}
				return false;
			})()
		`, anchorHash), &anchorStillVisible),
	); err != nil {
		t.Fatalf("failed to check anchor: %v", err)
	}
	if !anchorStillVisible {
		t.Fatal("anchor row is no longer visible after new events (viewport jumped)")
	}

	// Click the pill.
	if err := chromedp.Run(ctx,
		chromedp.Click(".feed-new-pill", chromedp.ByQuery),
		chromedp.Sleep(500*time.Millisecond),
	); err != nil {
		t.Fatalf("clicking pill failed: %v", err)
	}

	// Verify scroll returned to top and pill is hidden.
	var scrollTop float64
	var pillDisplay string
	if err := chromedp.Run(ctx,
		chromedp.Evaluate(`document.getElementById('viewport').scrollTop`, &scrollTop),
		chromedp.Evaluate(`document.querySelector('.feed-new-pill').style.display`, &pillDisplay),
	); err != nil {
		t.Fatalf("post-click assertions failed: %v", err)
	}
	if scrollTop != 0 {
		t.Fatalf("expected scrollTop 0 after pill click, got %f", scrollTop)
	}
	if pillDisplay != "none" {
		t.Fatalf("expected pill hidden after click, got display=%q", pillDisplay)
	}

	// Inject 3 more events — pill should NOT reappear (live mode).
	injectEvents(svc, 200, 3, "announced")
	time.Sleep(500 * time.Millisecond)

	if err := chromedp.Run(ctx,
		chromedp.Evaluate(`document.querySelector('.feed-new-pill').style.display`, &pillDisplay),
	); err != nil {
		t.Fatalf("live mode check failed: %v", err)
	}
	if pillDisplay != "none" {
		t.Fatalf("pill should stay hidden in live mode, got display=%q", pillDisplay)
	}
}

func TestFeedScrollPauseReentry(t *testing.T) {
	// Regression test: scrolling down a second time after returning to top
	// should start from the current position, not jump to the end.
	gethWS, svc := startMockGeth(t)
	txviewURL := startTxview(t, gethWS)
	ctx, _ := newBrowser(t)

	if err := chromedp.Run(ctx, chromedp.Navigate(txviewURL)); err != nil {
		t.Fatalf("navigate failed: %v", err)
	}
	waitSubscribed(t, svc)

	// Fill feed.
	injectEvents(svc, 1, 30, "announced")
	if err := chromedp.Run(ctx,
		chromedp.Poll(`
			(function() {
				var sp = document.getElementById('scroll-spacer');
				var vp = document.getElementById('viewport');
				return sp && vp && parseInt(sp.style.height) > vp.clientHeight;
			})()
		`, nil, chromedp.WithPollingInterval(100*time.Millisecond)),
	); err != nil {
		t.Fatalf("waiting for scrollable feed: %v", err)
	}

	// First scroll cycle: scroll down, then back to top.
	if err := chromedp.Run(ctx,
		chromedp.Evaluate(`document.getElementById('viewport').scrollTop = 200`, nil),
		chromedp.Sleep(300*time.Millisecond),
	); err != nil {
		t.Fatal(err)
	}

	// Inject events while paused.
	injectEvents(svc, 100, 5, "announced")
	time.Sleep(500 * time.Millisecond)

	// Return to top (unpause).
	if err := chromedp.Run(ctx,
		chromedp.Evaluate(`document.getElementById('viewport').scrollTop = 0`, nil),
		chromedp.Sleep(300*time.Millisecond),
	); err != nil {
		t.Fatal(err)
	}

	// Second scroll: scroll down by a small amount (100px).
	if err := chromedp.Run(ctx,
		chromedp.Evaluate(`document.getElementById('viewport').scrollTop = 100`, nil),
		chromedp.Sleep(300*time.Millisecond),
	); err != nil {
		t.Fatal(err)
	}

	// Verify scrollTop is near 100, not jumped to the end.
	var scrollTop float64
	if err := chromedp.Run(ctx,
		chromedp.Evaluate(`document.getElementById('viewport').scrollTop`, &scrollTop),
	); err != nil {
		t.Fatal(err)
	}

	// Allow some tolerance for scroll compensation from events arriving
	// between unpause and second scroll, but it should not jump far.
	if scrollTop > 500 {
		t.Fatalf("second scroll jumped to %f instead of staying near 100", scrollTop)
	}
}

func TestFeedScrollPauseCounterAccumulates(t *testing.T) {
	gethWS, svc := startMockGeth(t)
	txviewURL := startTxview(t, gethWS)
	ctx, _ := newBrowser(t)

	if err := chromedp.Run(ctx, chromedp.Navigate(txviewURL)); err != nil {
		t.Fatalf("navigate failed: %v", err)
	}
	waitSubscribed(t, svc)

	// Fill feed and make it scrollable.
	injectEvents(svc, 1, 30, "announced")
	if err := chromedp.Run(ctx,
		chromedp.Poll(`
			(function() {
				var sp = document.getElementById('scroll-spacer');
				var vp = document.getElementById('viewport');
				return sp && vp && parseInt(sp.style.height) > vp.clientHeight;
			})()
		`, nil, chromedp.WithPollingInterval(100*time.Millisecond)),
	); err != nil {
		t.Fatalf("waiting for scrollable feed: %v", err)
	}

	// Scroll down to pause.
	if err := chromedp.Run(ctx,
		chromedp.Evaluate(`document.getElementById('viewport').scrollTop = 200`, nil),
		chromedp.Sleep(300*time.Millisecond),
	); err != nil {
		t.Fatal(err)
	}

	// Inject 3 new hashes → pill shows "3 new".
	injectEvents(svc, 100, 3, "announced")
	if err := chromedp.Run(ctx,
		chromedp.Poll(`
			(function() {
				var p = document.querySelector('.feed-new-pill');
				return p && p.textContent.includes('3 new');
			})()
		`, nil, chromedp.WithPollingInterval(100*time.Millisecond)),
	); err != nil {
		t.Fatalf("pill did not show '3 new': %v", err)
	}

	// Inject 2 more new hashes → pill shows "5 new".
	injectEvents(svc, 200, 2, "announced")
	if err := chromedp.Run(ctx,
		chromedp.Poll(`
			(function() {
				var p = document.querySelector('.feed-new-pill');
				return p && p.textContent.includes('5 new');
			})()
		`, nil, chromedp.WithPollingInterval(100*time.Millisecond)),
	); err != nil {
		t.Fatalf("pill did not show '5 new': %v", err)
	}

	// Inject status update on existing hash → counter stays at 5.
	injectStatusUpdate(svc, 100, "pooled")
	time.Sleep(500 * time.Millisecond)
	var pillText string
	if err := chromedp.Run(ctx,
		chromedp.Evaluate(`document.querySelector('.feed-new-pill').textContent`, &pillText),
	); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(pillText, "5 new") {
		t.Fatalf("status update should not increment counter, pill says %q", pillText)
	}

	// Scroll to top → pill disappears.
	if err := chromedp.Run(ctx,
		chromedp.Evaluate(`document.getElementById('viewport').scrollTop = 0`, nil),
		chromedp.Sleep(500*time.Millisecond),
	); err != nil {
		t.Fatal(err)
	}

	var pillDisplay string
	if err := chromedp.Run(ctx,
		chromedp.Evaluate(`document.querySelector('.feed-new-pill').style.display`, &pillDisplay),
	); err != nil {
		t.Fatal(err)
	}
	if pillDisplay != "none" {
		t.Fatalf("pill should be hidden after scroll to top, got display=%q", pillDisplay)
	}
}

func TestFeedFilterByStatus(t *testing.T) {
	gethWS, svc := startMockGeth(t)
	txviewURL := startTxview(t, gethWS)
	ctx, _ := newBrowser(t)

	if err := chromedp.Run(ctx, chromedp.Navigate(txviewURL)); err != nil {
		t.Fatalf("navigate failed: %v", err)
	}
	waitSubscribed(t, svc)

	// Inject mixed events: 3 announced, 2 pooled, 1 included.
	injectEvents(svc, 1, 3, "announced")
	injectEvents(svc, 10, 2, "pooled")
	injectEvents(svc, 20, 1, "included")

	// Wait for all 6 to render.
	if err := chromedp.Run(ctx,
		chromedp.Poll(`
			(function() {
				var el = document.getElementById('cnt-total');
				return el && parseInt(el.textContent) >= 6;
			})()
		`, nil, chromedp.WithPollingInterval(100*time.Millisecond)),
	); err != nil {
		t.Fatalf("events did not arrive: %v", err)
	}

	// Select "pooled" filter.
	if err := chromedp.Run(ctx,
		chromedp.SetValue("#filter-status", "pooled", chromedp.ByQuery),
		chromedp.Evaluate(`
			document.getElementById('filter-status').dispatchEvent(new Event('change'))
		`, nil),
		chromedp.Sleep(500*time.Millisecond),
	); err != nil {
		t.Fatalf("filter selection failed: %v", err)
	}

	// Count visible rows.
	var rowCount int
	if err := chromedp.Run(ctx,
		chromedp.Evaluate(`document.getElementById('row-container').children.length`, &rowCount),
	); err != nil {
		t.Fatal(err)
	}
	if rowCount != 2 {
		t.Fatalf("expected 2 pooled rows, got %d", rowCount)
	}
}

func TestTabSwitching(t *testing.T) {
	gethWS, _ := startMockGeth(t)
	txviewURL := startTxview(t, gethWS)
	ctx, _ := newBrowser(t)

	if err := chromedp.Run(ctx, chromedp.Navigate(txviewURL)); err != nil {
		t.Fatalf("navigate failed: %v", err)
	}

	// Click "Top" tab.
	if err := chromedp.Run(ctx,
		chromedp.Click(`button[data-tab="top"]`, chromedp.ByQuery),
		chromedp.Sleep(300*time.Millisecond),
	); err != nil {
		t.Fatal(err)
	}

	var topVisible, feedVisible bool
	if err := chromedp.Run(ctx,
		chromedp.Evaluate(`getComputedStyle(document.getElementById('view-top')).display !== 'none'`, &topVisible),
		chromedp.Evaluate(`getComputedStyle(document.getElementById('view-feed')).display !== 'none'`, &feedVisible),
	); err != nil {
		t.Fatal(err)
	}
	if !topVisible {
		t.Fatal("Top view should be visible after clicking Top tab")
	}
	if feedVisible {
		t.Fatal("Feed view should be hidden after clicking Top tab")
	}

	// Click "Feed" tab to go back.
	if err := chromedp.Run(ctx,
		chromedp.Click(`button[data-tab="feed"]`, chromedp.ByQuery),
		chromedp.Sleep(300*time.Millisecond),
	); err != nil {
		t.Fatal(err)
	}

	if err := chromedp.Run(ctx,
		chromedp.Evaluate(`getComputedStyle(document.getElementById('view-feed')).display !== 'none'`, &feedVisible),
		chromedp.Evaluate(`getComputedStyle(document.getElementById('view-top')).display !== 'none'`, &topVisible),
	); err != nil {
		t.Fatal(err)
	}
	if !feedVisible {
		t.Fatal("Feed view should be visible after clicking Feed tab")
	}
	if topVisible {
		t.Fatal("Top view should be hidden after clicking Feed tab")
	}
}
