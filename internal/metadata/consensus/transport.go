package consensus

import (
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"

	"github.com/hashicorp/raft"
)

const (
	transportMaxPool = 3
	transportTimeout = 10 * time.Second
)

var ErrInvalidAdvertiseAddr = errors.New("invalid raft advertise address")

// advertiseAddr lets the transport advertise a hostname. raft.NewTCPTransport
// only accepts a resolved IP, which goes stale whenever a Kubernetes pod is
// rescheduled and is useless as a leader hint for clients that only know DNS names.
type advertiseAddr string

func (a advertiseAddr) Network() string { return "tcp" }
func (a advertiseAddr) String() string  { return string(a) }

type advertisingStreamLayer struct {
	listener  net.Listener
	advertise net.Addr
}

func (s *advertisingStreamLayer) Accept() (net.Conn, error) { return s.listener.Accept() }
func (s *advertisingStreamLayer) Close() error              { return s.listener.Close() }
func (s *advertisingStreamLayer) Addr() net.Addr            { return s.advertise }

func (s *advertisingStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", string(address), timeout)
}

func validateAdvertiseAddr(addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("%w %q: %v", ErrInvalidAdvertiseAddr, addr, err)
	}
	if host == "" {
		return fmt.Errorf("%w %q: host is required", ErrInvalidAdvertiseAddr, addr)
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsUnspecified() {
		return fmt.Errorf("%w %q: unspecified IP cannot be advertised", ErrInvalidAdvertiseAddr, addr)
	}
	if n, err := strconv.Atoi(port); err != nil || n <= 0 || n > 65535 {
		return fmt.Errorf("%w %q: invalid port", ErrInvalidAdvertiseAddr, addr)
	}
	return nil
}

// newRaftTransport binds to bindAddr. With an empty advertise it keeps the
// original behaviour of resolving bindAddr and advertising that IP.
func newRaftTransport(bindAddr, advertise string, logOutput io.Writer) (*raft.NetworkTransport, error) {
	if advertise == "" {
		resolved, err := net.ResolveTCPAddr("tcp", bindAddr)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve addr : %s, error: %w", bindAddr, err)
		}
		transport, err := raft.NewTCPTransport(resolved.String(), nil, transportMaxPool, transportTimeout, logOutput)
		if err != nil {
			return nil, fmt.Errorf("%w: addr : %s", err, resolved.String())
		}
		return transport, nil
	}

	if err := validateAdvertiseAddr(advertise); err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return nil, fmt.Errorf("listen on raft addr %s: %w", bindAddr, err)
	}
	stream := &advertisingStreamLayer{listener: listener, advertise: advertiseAddr(advertise)}
	return raft.NewNetworkTransport(stream, transportMaxPool, transportTimeout, logOutput), nil
}
