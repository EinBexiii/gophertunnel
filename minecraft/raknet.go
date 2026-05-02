package minecraft

import (
	"context"
	"github.com/sandertv/go-raknet"
	"log/slog"
	"net"
	"time"
)

// RakNet is an implementation of a RakNet v10 Network.
type RakNet struct {
	l *slog.Logger
	// MTUDiscoveryWarmup is forwarded to raknet.Dialer; see its documentation.
	MTUDiscoveryWarmup time.Duration
}

// NewRakNet constructs a RakNet network with the given logger and MTU
// discovery warmup duration. Use the returned value when registering a
// custom network with RegisterNetwork.
func NewRakNet(l *slog.Logger, mtuDiscoveryWarmup time.Duration) RakNet {
	return RakNet{l: l, MTUDiscoveryWarmup: mtuDiscoveryWarmup}
}

// DialContext ...
func (r RakNet) DialContext(ctx context.Context, address string) (net.Conn, error) {
	return raknet.Dialer{
		ErrorLog:           r.l.With("net origin", "raknet"),
		MTUDiscoveryWarmup: r.MTUDiscoveryWarmup,
	}.DialContext(ctx, address)
}

// PingContext ...
func (r RakNet) PingContext(ctx context.Context, address string) (response []byte, err error) {
	return raknet.Dialer{ErrorLog: r.l.With("net origin", "raknet")}.PingContext(ctx, address)
}

// Listen ...
func (r RakNet) Listen(address string) (NetworkListener, error) {
	return raknet.ListenConfig{ErrorLog: r.l.With("net origin", "raknet")}.Listen(address)
}

// init registers the RakNet network.
func init() {
	RegisterNetwork("raknet", func(l *slog.Logger) Network { return RakNet{l: l} })
}
