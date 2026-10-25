package net

import (
	"errors"
	"net"
	"net/http"
	"sync"
	"time"

	"golang.org/x/net/websocket"
)

var ErrWebsocketListenerClosed = errors.New("websocket listener closed")

const (
	FrpWebsocketPath = "/~!frp"

	// websocketCloseTimeout bounds how long Close may wait for the websocket
	// close frame to be written to the transport.
	websocketCloseTimeout = time.Second
)

// boundedCloseConn bounds the time Close takes on a connection whose peer
// stopped reading.
//
// golang.org/x/net/websocket's Conn.Close writes a close frame before it
// closes the transport, and that write has to take the same mutex that a
// pending write holds. When the peer stopped draining the connection, the
// pending write blocks, so the close blocks as well and the transport is
// never closed. The same problem shows up when the connection is closed while
// its send buffer still holds unsent data.
//
// A write deadline bounds that wait. The pending write fails, the mutex is
// released, and the close frame write then fails fast instead of blocking.
type boundedCloseConn struct {
	net.Conn
	timeout time.Duration
}

// WrapBoundedCloseConn returns a connection whose Close is bounded by the
// given timeout. A non-positive timeout leaves the close unbounded.
func WrapBoundedCloseConn(c net.Conn, timeout time.Duration) net.Conn {
	return &boundedCloseConn{Conn: c, timeout: timeout}
}

func (c *boundedCloseConn) Close() error {
	if c.timeout > 0 {
		// The deadline also applies to a write that is already pending, which
		// is what releases the websocket write mutex.
		_ = c.SetWriteDeadline(time.Now().Add(c.timeout))
	}
	return c.Conn.Close()
}

type WebsocketListener struct {
	ln       net.Listener
	acceptCh chan net.Conn
	done     chan struct{}

	server *http.Server

	closeOnce sync.Once
}

// NewWebsocketListener to handle websocket connections
// ln: tcp listener for websocket connections
func NewWebsocketListener(ln net.Listener) (wl *WebsocketListener) {
	wl = &WebsocketListener{
		ln:       ln,
		acceptCh: make(chan net.Conn),
		done:     make(chan struct{}),
	}

	muxer := http.NewServeMux()
	muxer.Handle(FrpWebsocketPath, websocket.Handler(func(c *websocket.Conn) {
		// The tunnel payload is a raw byte stream (yamux), not UTF-8 text.
		// Send it as binary frames; otherwise RFC 6455-compliant intermediaries
		// (e.g. API gateways/reverse proxies) UTF-8-validate the default text
		// frames and close the connection on invalid bytes.
		c.PayloadType = websocket.BinaryFrame
		notifyCh := make(chan struct{})
		// The bounded close has to wrap the notify conn: the deadline it sets
		// reaches the websocket connection through the embedded net.Conn, and
		// it has to be in place before the close frame is written.
		conn := WrapBoundedCloseConn(
			WrapCloseNotifyConn(c, func(_ error) {
				close(notifyCh)
			}),
			websocketCloseTimeout,
		)
		select {
		case <-wl.done:
			conn.Close()
			return
		case wl.acceptCh <- conn:
			// A ready handoff may win when Close races with it. Once delivered,
			// the connection is owned by the caller of Accept.
		}
		<-notifyCh
	}))

	wl.server = &http.Server{
		Addr:              ln.Addr().String(),
		Handler:           muxer,
		ReadHeaderTimeout: 60 * time.Second,
	}

	go func() {
		_ = wl.server.Serve(ln)
	}()
	return
}

func (p *WebsocketListener) Accept() (net.Conn, error) {
	select {
	case <-p.done:
		return nil, ErrWebsocketListenerClosed
	default:
	}
	select {
	case <-p.done:
		return nil, ErrWebsocketListenerClosed
	case c := <-p.acceptCh:
		return c, nil
	}
}

func (p *WebsocketListener) Close() error {
	p.closeOnce.Do(func() { close(p.done) })
	return p.server.Close()
}

func (p *WebsocketListener) Addr() net.Addr {
	return p.ln.Addr()
}
