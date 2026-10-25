// Copyright 2026 The frp Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package net

import (
	"net"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/websocket"
)

// newUnreadingWebsocketPeer starts a websocket server that never reads from
// the client. A client write then fills the transport send buffer and blocks
// inside the websocket write, holding the websocket write mutex.
func newUnreadingWebsocketPeer(t *testing.T) (*websocket.Conn, net.Conn) {
	t.Helper()
	release := make(chan struct{})
	srv := httptest.NewServer(websocket.Handler(func(c *websocket.Conn) {
		// Never read, never answer a close frame.
		<-release
	}))
	t.Cleanup(func() {
		close(release)
		srv.Close()
	})

	u, err := url.Parse(srv.URL)
	require.NoError(t, err)
	cfg, err := websocket.NewConfig("ws://"+u.Host+FrpWebsocketPath, "http://"+u.Host)
	require.NoError(t, err)

	raw, err := net.Dial("tcp", u.Host)
	require.NoError(t, err)
	t.Cleanup(func() { raw.Close() })

	ws, err := websocket.NewClient(cfg, raw)
	require.NoError(t, err)
	return ws, raw
}

// blockWebsocketWrite starts a write that fills the transport send buffer and
// therefore blocks while holding the websocket write mutex.
func blockWebsocketWrite(t *testing.T, ws *websocket.Conn) <-chan error {
	t.Helper()
	errCh := make(chan error, 1)
	go func() {
		payload := make([]byte, 1024*1024)
		for {
			if _, err := ws.Write(payload); err != nil {
				errCh <- err
				return
			}
		}
	}()
	// The first write is far larger than the initial send buffer, so the
	// writer is blocked in the transport write and holds the websocket write
	// mutex.
	time.Sleep(200 * time.Millisecond)
	return errCh
}

// TestBoundedCloseConnReturnsWhilePeerStoppedReading verifies that the close
// of a websocket connection whose peer stopped reading does not block.
//
// The websocket close frame write shares its mutex with the pending write, so
// without a bound the close waits for the pending write forever and the
// transport is never closed.
func TestBoundedCloseConnReturnsWhilePeerStoppedReading(t *testing.T) {
	ws, _ := newUnreadingWebsocketPeer(t)
	writeErr := blockWebsocketWrite(t, ws)

	conn := WrapBoundedCloseConn(ws, 200*time.Millisecond)
	closeErr := make(chan error, 1)
	go func() { closeErr <- conn.Close() }()

	select {
	case err := <-closeErr:
		// The close frame cannot be delivered, the close itself must still
		// return instead of blocking.
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("bounded close blocked while the peer stopped reading")
	}

	// The pending write has to be released as well.
	select {
	case <-writeErr:
	case <-time.After(5 * time.Second):
		t.Fatal("pending write was not released")
	}
}

// TestUnboundedCloseConnBlocksWhilePeerStoppedReading is the baseline for
// TestBoundedCloseConnReturnsWhilePeerStoppedReading: without the bound the
// close really does block.
func TestUnboundedCloseConnBlocksWhilePeerStoppedReading(t *testing.T) {
	ws, raw := newUnreadingWebsocketPeer(t)
	blockWebsocketWrite(t, ws)
	t.Cleanup(func() { raw.Close() })

	conn := WrapBoundedCloseConn(ws, 0)
	closeErr := make(chan error, 1)
	go func() { closeErr <- conn.Close() }()

	select {
	case err := <-closeErr:
		t.Fatalf("unbounded close returned early: %v", err)
	case <-time.After(time.Second):
		// Expected: the close is still blocked.
	}
}
