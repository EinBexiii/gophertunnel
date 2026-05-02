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
	// MaxAttempts is forwarded to raknet.Dialer; see its documentation.
	MaxAttempts int
	// WarmupOnFirstAttempt is forwarded to raknet.Dialer; see its documentation.
	WarmupOnFirstAttempt bool
	// MaxMTU is forwarded to raknet.Dialer; see its documentation.
	MaxMTU uint16
}

// NewRakNet constructs a RakNet network with the given logger and MTU
// discovery warmup duration. Use the returned value when registering a
// custom network with RegisterNetwork.
//
// For full control (incl. MaxAttempts) construct RakNet directly via
// NewRakNetWith.
func NewRakNet(l *slog.Logger, mtuDiscoveryWarmup time.Duration) RakNet {
	return RakNet{l: l, MTUDiscoveryWarmup: mtuDiscoveryWarmup}
}

// NewRakNetWith constructs a RakNet network with the given logger, MTU
// discovery warmup duration, and maximum number of dial attempts.
func NewRakNetWith(l *slog.Logger, mtuDiscoveryWarmup time.Duration, maxAttempts int) RakNet {
	return RakNet{l: l, MTUDiscoveryWarmup: mtuDiscoveryWarmup, MaxAttempts: maxAttempts}
}

// WithLogger returns a copy of r with the logger set to l. This is intended
// to be used inside a RegisterNetwork factory: build a RakNet with all the
// fields set, then call .WithLogger(l) inside the factory.
func (r RakNet) WithLogger(l *slog.Logger) RakNet {
	r.l = l
	return r
}

// DialContext ...
func (r RakNet) DialContext(ctx context.Context, address string) (net.Conn, error) {
	return raknet.Dialer{
		ErrorLog:             r.l.With("net origin", "raknet"),
		MTUDiscoveryWarmup:   r.MTUDiscoveryWarmup,
		MaxAttempts:          r.MaxAttempts,
		WarmupOnFirstAttempt: r.WarmupOnFirstAttempt,
		MaxMTU:               r.MaxMTU,
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
