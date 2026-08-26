package packet

// BatchHeaderer may be implemented by transports that need a custom batch prefix.
//
// The default Minecraft packet transport uses the standard batch header. Transports
// such as NetherNet can return nil when packets are already framed by the transport.
type BatchHeaderer interface {
	// BatchHeader returns the bytes expected before each packet batch.
	BatchHeader() []byte
}

// EncryptionDisabler may be implemented by transports that must not use
// Minecraft packet encryption.
//
// This is intended for transports that already provide their own encryption.
type EncryptionDisabler interface {
	// DisableEncryption reports whether Minecraft packet encryption should be disabled.
	DisableEncryption() bool
}

// PacketReader may be implemented by transports that can read complete packet
// payloads directly.
type PacketReader interface {
	// ReadPacket reads one complete packet payload from the transport.
	ReadPacket() ([]byte, error)
}

// UnreliableWriter may be implemented by transports that can deliver a
// packet batch without guaranteed or ordered delivery.
//
// Minecraft packet encryption cannot be shared between two Encoders: each keeps its own CTR stream, and the
// peer's Decoder tracks a single counter for the connection. Conn therefore only builds an unreliable sink
// for a transport that also implements EncryptionDisabler and disables Minecraft's own encryption, i.e. one
// that provides its own encryption instead.
type UnreliableWriter interface {
	// WriteUnreliable writes one complete packet batch with relaxed
	// delivery guarantees. Implementations fall back to reliable
	// delivery when the batch cannot be sent unreliably.
	WriteUnreliable(b []byte) (n int, err error)
}

// TransportCapabilities is the full set of optional packet transport methods.
//
// Normal transports do not need to implement this interface. If code wraps a
// connection that has any of these methods, the wrapper must expose the same
// methods too so Encoder and Decoder keep using the transport-specific behavior.
type TransportCapabilities interface {
	BatchHeaderer
	EncryptionDisabler
	PacketReader
	UnreliableWriter
}
