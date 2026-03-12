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

// txview is a web-based transaction lifecycle viewer for geth's txtracker.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	stdlog "log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/ethereum/go-ethereum/cmd/txview/internal/ui"
	"github.com/ethereum/go-ethereum/internal/flags"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/urfave/cli/v2"
)

var (
	rpcFlag = &cli.StringFlag{
		Name:  "rpc",
		Usage: "Geth WebSocket RPC endpoint",
		Value: "ws://localhost:8546",
	}
	addrFlag = &cli.StringFlag{
		Name:  "addr",
		Usage: "Local HTTP listen address",
		Value: "localhost:8670",
	}
)

var app = flags.NewApp("transaction lifecycle viewer for geth txtracker")

func init() {
	app.Flags = []cli.Flag{rpcFlag, addrFlag}
	app.Action = run
}

func main() {
	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx *cli.Context) error {
	endpoint := ctx.String("rpc")
	addr := ctx.String("addr")

	// Verify geth connection at startup.
	client, err := rpc.Dial(endpoint)
	if err != nil {
		return fmt.Errorf("failed to connect to geth at %s: %w", endpoint, err)
	}
	client.Close()

	// Reverse proxy for /ws so the browser connects same-origin (avoids
	// geth's WebSocket origin check).
	httpEndpoint := strings.Replace(strings.Replace(endpoint, "ws://", "http://", 1), "wss://", "https://", 1)
	target, err := url.Parse(httpEndpoint)
	if err != nil {
		return fmt.Errorf("invalid RPC endpoint URL: %w", err)
	}
	wsProxy := &httputil.ReverseProxy{
		Rewrite: func(r *httputil.ProxyRequest) {
			r.SetURL(target)
			// SetURL preserves the incoming path (/ws), but geth serves
			// WebSocket at the root. Override to the target's path.
			r.Out.URL.Path = target.Path
			r.Out.URL.RawPath = ""
			// Remove Origin header so geth skips the origin check.
			r.Out.Header.Del("Origin")
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			stdlog.Printf("[proxy] error: %s %s: %v", r.Method, r.URL.String(), err)
			http.Error(w, err.Error(), http.StatusBadGateway)
		},
	}
	http.Handle("/ws", wsProxy)

	// Config endpoint tells the JS to use the proxied /ws path.
	http.HandleFunc("/config", func(w http.ResponseWriter, r *http.Request) {
		scheme := "ws"
		if r.TLS != nil {
			scheme = "wss"
		}
		wsURL := scheme + "://" + r.Host + "/ws"
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"rpcEndpoint": wsURL,
		})
	})

	// Serve standalone transaction detail page for /tx/0x... URLs.
	http.HandleFunc("/tx/", func(w http.ResponseWriter, r *http.Request) {
		f, err := ui.Assets.Open("detail.html")
		if err != nil {
			http.NotFound(w, r)
			return
		}
		defer f.Close()
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		io.Copy(w, f)
	})

	// Serve embedded UI assets.
	assets, err := fs.Sub(ui.Assets, ".")
	if err != nil {
		return err
	}
	http.Handle("/", http.FileServerFS(assets))

	srv := &http.Server{Addr: addr}

	fmt.Printf("txview listening on http://%s\n", addr)
	fmt.Printf("  Geth RPC: %s (proxied at /ws)\n", endpoint)
	fmt.Println("  Ensure geth is started with: --ws --ws.api txtracker")
	fmt.Println()

	// Graceful shutdown on SIGINT/SIGTERM.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\nShutting down...")
		srv.Shutdown(context.Background())
	}()

	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		return err
	}
	return nil
}
