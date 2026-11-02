package server

import "net"

// BufferedListener wraps a listener and sets each accepted TCP connection's kernel send buffer.
// A small SO_SNDBUF makes back-pressure from a slow consumer reach the hub within milliseconds
// instead of after megabytes of kernel buffering, so the slow-consumer policy actually engages
// and staleness stays bounded.
type BufferedListener struct {
	net.Listener
	WriteBuffer int
}

// Accept implements net.Listener.
func (l BufferedListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	if tc, ok := c.(*net.TCPConn); ok && l.WriteBuffer > 0 {
		_ = tc.SetWriteBuffer(l.WriteBuffer)
		_ = tc.SetNoDelay(true)
	}
	return c, nil
}
