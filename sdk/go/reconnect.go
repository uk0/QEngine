package tsdb

import (
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"
)

// reconnectBackoff is the per-attempt wait before re-dialing.  Index i is used
// before attempt i+1.  Kept short: a peer restart or network blip usually
// recovers within a few hundred milliseconds.
var reconnectBackoff = []time.Duration{100 * time.Millisecond, 300 * time.Millisecond}

// isConnError reports whether err is a transport-level failure that warrants a
// reconnect, as opposed to a server application error (an MsgError frame, which
// decodeError turns into a normal error and which must NOT trigger a reconnect).
//
// Reconnect-worthy: the peer went away or the socket broke — io.EOF /
// io.ErrUnexpectedEOF (half-open read), "broken pipe" / "connection reset"
// (write/read after peer close), or any net.OpError on the connection.
func isConnError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return true
	}
	// Some platforms surface these as bare syscall errnos wrapped in
	// os.SyscallError without a net.OpError; match on text as a fallback.
	msg := err.Error()
	if strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "use of closed network connection") {
		return true
	}
	return false
}

// reconnect tears down the dead connection and re-establishes a fresh session
// on the retained address: re-dial (Dial, or DialTLS with the retained
// tls.Config when the client was opened via OpenTLS), re-send HELLO
// exactly as Open did, and — if the original session authenticated — re-Login
// with the retained credentials so the new socket carries a valid server-side
// token.
//
// It retries the dial/handshake up to MaxReconnect times with a short backoff.
// On success c.Conn points at the new connection; on failure the last error is
// returned and c.Conn is left nil.
func (c *Client) reconnect() error {
	attempts := c.maxReconnect()
	if attempts <= 0 {
		return errors.New("auto-reconnect disabled")
	}
	if c.Conn != nil {
		_ = c.Conn.Close()
	}
	var lastErr error
	for i := 0; i < attempts; i++ {
		d := i
		if d >= len(reconnectBackoff) {
			d = len(reconnectBackoff) - 1
		}
		time.Sleep(reconnectBackoff[d])
		if err := c.dialAndHandshake(); err != nil {
			lastErr = err
			continue
		}
		return nil
	}
	if lastErr == nil {
		lastErr = errors.New("reconnect failed")
	}
	return lastErr
}

// dialAndHandshake performs one full open: Dial + HELLO + (optional) re-Login.
// On any failure it closes the partial connection and returns the error.
func (c *Client) dialAndHandshake() error {
	conn, err := c.dial()
	if err != nil {
		return err
	}
	if _, err := conn.Send(MsgHello, 0, 0, nil); err != nil {
		conn.Close()
		return err
	}
	f, err := conn.Recv()
	if err != nil {
		conn.Close()
		return err
	}
	if f.Type != MsgHelloOK {
		conn.Close()
		return fmt.Errorf("unexpected HELLO response type=%d", f.Type)
	}
	c.Conn = conn
	// Re-establish auth on the fresh socket if the original session logged in.
	// Login attaches the token server-side to this connection; the prior token
	// died with the old socket, so a new Login is mandatory, not optional.
	if c.loggedIn {
		if err := c.Login(c.user, c.pass); err != nil {
			conn.Close()
			c.Conn = nil
			return err
		}
	}
	return nil
}

// maxReconnect returns the effective retry budget: MaxReconnect when zero falls
// back to the default of 2 attempts (auto-reconnect is on by default).  A
// negative MaxReconnect disables reconnect entirely (opt-out).
func (c *Client) maxReconnect() int {
	if c.MaxReconnect == 0 {
		return 2
	}
	if c.MaxReconnect < 0 {
		return 0
	}
	return c.MaxReconnect
}

// withReconnect runs op (a single Send+Recv round-trip).  If op fails with a
// transport error (isConnError) and auto-reconnect is enabled, it reconnects and
// retries op exactly once.  A server application error (MsgError, surfaced by
// decodeError) is returned as-is and never triggers a reconnect.
//
// retrySafe must be true only when re-running op after a reconnect is harmless.
// It is true for read-only operations (Query) and for writes whose failure is
// provably before any server-side effect (send/dial failure).  When false, a
// transport error still triggers a reconnect (so the next call lands on a live
// socket) but op is NOT retried — the original error is returned so the caller
// can decide whether to resend.
func (c *Client) withReconnect(retrySafe bool, op func() error) error {
	err := op()
	if err == nil || !isConnError(err) || c.maxReconnect() <= 0 {
		return err
	}
	if rcErr := c.reconnect(); rcErr != nil {
		// Surface the original transport error; the reconnect failure is
		// secondary and the caller mainly cares that the op did not complete.
		return err
	}
	if !retrySafe {
		return err
	}
	return op()
}
