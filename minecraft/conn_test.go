package minecraft

import (
	"bytes"
	"encoding/hex"
	"io"
	"log/slog"
	"math"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/sandertv/gophertunnel/minecraft/internal"
	"github.com/sandertv/gophertunnel/minecraft/protocol"
	"github.com/sandertv/gophertunnel/minecraft/protocol/packet"
)

// fakeAddr is a minimal net.Addr used by the fake transports below.
type fakeAddr struct{}

func (fakeAddr) Network() string { return "fake" }
func (fakeAddr) String() string  { return "fake" }

// fakeReliableConn is a net.Conn double that only ever receives reliable writes. It does not implement
// packet.UnreliableWriter, matching every transport gophertunnel supported before this capability existed.
type fakeReliableConn struct {
	mu       sync.Mutex
	reliable [][]byte
}

func (f *fakeReliableConn) Write(b []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reliable = append(f.reliable, append([]byte(nil), b...))
	return len(b), nil
}
func (f *fakeReliableConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (f *fakeReliableConn) Close() error                     { return nil }
func (f *fakeReliableConn) LocalAddr() net.Addr              { return fakeAddr{} }
func (f *fakeReliableConn) RemoteAddr() net.Addr             { return fakeAddr{} }
func (f *fakeReliableConn) SetDeadline(time.Time) error      { return nil }
func (f *fakeReliableConn) SetReadDeadline(time.Time) error  { return nil }
func (f *fakeReliableConn) SetWriteDeadline(time.Time) error { return nil }

func (f *fakeReliableConn) batches() [][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]byte(nil), f.reliable...)
}

// fakeUnreliableConn is a net.Conn double that additionally implements packet.UnreliableWriter and
// packet.EncryptionDisabler, matching a transport such as NetherNet. It records reliable and unreliable
// batches separately, in the order they arrive, so tests can assert on partitioning and per-sink ordering.
type fakeUnreliableConn struct {
	mu         sync.Mutex
	reliable   [][]byte
	unreliable [][]byte
	disableEnc bool
}

func (f *fakeUnreliableConn) Write(b []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reliable = append(f.reliable, append([]byte(nil), b...))
	return len(b), nil
}
func (f *fakeUnreliableConn) WriteUnreliable(b []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.unreliable = append(f.unreliable, append([]byte(nil), b...))
	return len(b), nil
}
func (f *fakeUnreliableConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (f *fakeUnreliableConn) Close() error                     { return nil }
func (f *fakeUnreliableConn) LocalAddr() net.Addr              { return fakeAddr{} }
func (f *fakeUnreliableConn) RemoteAddr() net.Addr             { return fakeAddr{} }
func (f *fakeUnreliableConn) SetDeadline(time.Time) error      { return nil }
func (f *fakeUnreliableConn) SetReadDeadline(time.Time) error  { return nil }
func (f *fakeUnreliableConn) SetWriteDeadline(time.Time) error { return nil }

// DisableEncryption implements packet.EncryptionDisabler. The zero value keeps encryption allowed, matching
// a transport that has no opinion on Minecraft's own encryption; such a transport must never get an
// unreliable encoder (see TestUnreliableEnc_RequiresEncryptionDisabler).
func (f *fakeUnreliableConn) DisableEncryption() bool { return f.disableEnc }

func (f *fakeUnreliableConn) batches() (reliable, unreliable [][]byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]byte(nil), f.reliable...), append([][]byte(nil), f.unreliable...)
}

// unreliableConnNoEncryptionDisabler implements packet.UnreliableWriter but not packet.EncryptionDisabler,
// matching a transport that supports unordered delivery without taking a position on Minecraft's own
// encryption. Conn must never build an unreliable encoder for a transport like this.
type unreliableConnNoEncryptionDisabler struct {
	mu         sync.Mutex
	reliable   [][]byte
	unreliable [][]byte
}

func (f *unreliableConnNoEncryptionDisabler) Write(b []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reliable = append(f.reliable, append([]byte(nil), b...))
	return len(b), nil
}
func (f *unreliableConnNoEncryptionDisabler) WriteUnreliable(b []byte) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.unreliable = append(f.unreliable, append([]byte(nil), b...))
	return len(b), nil
}
func (f *unreliableConnNoEncryptionDisabler) Read([]byte) (int, error)         { return 0, io.EOF }
func (f *unreliableConnNoEncryptionDisabler) Close() error                     { return nil }
func (f *unreliableConnNoEncryptionDisabler) LocalAddr() net.Addr              { return fakeAddr{} }
func (f *unreliableConnNoEncryptionDisabler) RemoteAddr() net.Addr             { return fakeAddr{} }
func (f *unreliableConnNoEncryptionDisabler) SetDeadline(time.Time) error      { return nil }
func (f *unreliableConnNoEncryptionDisabler) SetReadDeadline(time.Time) error  { return nil }
func (f *unreliableConnNoEncryptionDisabler) SetWriteDeadline(time.Time) error { return nil }

func (f *unreliableConnNoEncryptionDisabler) batches() (reliable, unreliable [][]byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]byte(nil), f.reliable...), append([][]byte(nil), f.unreliable...)
}

// newTestLogger returns a slog.Logger that discards everything, matching the default used by Dialer and
// Listener when no ErrorLog is configured.
func newTestLogger() *slog.Logger {
	return slog.New(internal.DiscardHandler{})
}

// testPacketSequence returns a small, deterministic sequence of packets used to exercise WritePacket. Both
// packet types marshal a single primitive field, keeping the expected wire bytes easy to reason about.
func testPacketSequence() []packet.Packet {
	return []packet.Packet{
		&packet.SetTime{Time: 42},
		&packet.SetDifficulty{Difficulty: 7},
		&packet.SetTime{Time: 1000},
	}
}

// goldenReliableBatchHex is the exact hex-encoded byte sequence produced by a single Flush() of
// testPacketSequence() written to an unmodified v1.59.0 Conn (no compression, no encryption, no unreliable
// capability). It pins WritePacket/Flush's reliable-path framing so any future change to that path is
// caught here rather than by a live server.
//
// It was captured by checking out the v1.59.0 tag, running WritePacket/Flush for testPacketSequence()
// against a bare fake net.Conn (no capability), and hex-encoding the single resulting batch. There is
// deliberately no test in this package that regenerates this constant: that would let a change to the
// reliable path rewrite its own golden value instead of being caught by
// TestWritePacket_ByteIdentity_NoUnreliableCapability and TestWritePacket_ByteIdentity_NilPolicy below.
const goldenReliableBatchHex = "fe020a54023c07030ad00f"

func mustGoldenBytes(t *testing.T) []byte {
	t.Helper()
	b, err := hex.DecodeString(goldenReliableBatchHex)
	if err != nil {
		t.Fatalf("decode golden bytes: %v", err)
	}
	return b
}

// decodeBatchIDs decodes a single flushed batch and returns the packet ID of each packet it holds, in
// order. compression may be nil for an uncompressed batch. It fails the test on any decode error.
func decodeBatchIDs(t *testing.T, batch []byte, compression packet.Compression) []uint32 {
	t.Helper()
	dec := packet.NewDecoder(bytes.NewReader(batch))
	if compression != nil {
		dec.EnableCompression(compression, math.MaxInt)
	}
	packets, err := dec.Decode()
	if err != nil {
		t.Fatalf("decode batch: %v", err)
	}
	ids := make([]uint32, 0, len(packets))
	for _, pk := range packets {
		var hdr packet.Header
		if err := hdr.Read(bytes.NewReader(pk)); err != nil {
			t.Fatalf("decode packet header: %v", err)
		}
		ids = append(ids, hdr.PacketID)
	}
	return ids
}

// encodeRawPacket serialises pk into the raw header+payload bytes a caller with its own pre-serialised
// packet data (such as a proxy forwarding packets it never decoded) would pass to Conn.Write directly.
func encodeRawPacket(conn *Conn, pk packet.Packet) []byte {
	buf := new(bytes.Buffer)
	hdr := &packet.Header{PacketID: pk.ID()}
	_ = hdr.Write(buf)
	pk.Marshal(conn.proto.NewWriter(buf, conn.shieldID.Load()))
	return buf.Bytes()
}

// TestWritePacket_ByteIdentity_NoUnreliableCapability pins the core invariant: when the underlying
// net.Conn does not implement packet.UnreliableWriter, WritePacket/Flush must produce byte-for-byte the
// same output as unmodified v1.59.0, because every packet always takes the reliable path.
func TestWritePacket_ByteIdentity_NoUnreliableCapability(t *testing.T) {
	fake := &fakeReliableConn{}
	conn := newConn(fake, nil, newTestLogger(), proto{}, 0, false)

	for _, pk := range testPacketSequence() {
		if err := conn.WritePacket(pk); err != nil {
			t.Fatalf("write packet: %v", err)
		}
	}
	if err := conn.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	batches := fake.batches()
	if len(batches) != 1 {
		t.Fatalf("expected a single flushed batch, got %d", len(batches))
	}
	if want := mustGoldenBytes(t); !bytes.Equal(batches[0], want) {
		t.Fatalf("reliable batch diverged from golden v1.59.0 bytes:\ngot:  %x\nwant: %x", batches[0], want)
	}
}

// TestWritePacket_ByteIdentity_NilPolicy pins the same invariant for a transport that DOES implement
// packet.UnreliableWriter and disables Minecraft encryption, so Conn does build an unreliableEnc for it, but
// has no UnreliablePackets policy configured (the default). Every packet must still go out reliably,
// byte-for-byte identical to v1.59.0, and nothing may be written unreliably.
func TestWritePacket_ByteIdentity_NilPolicy(t *testing.T) {
	fake := &fakeUnreliableConn{disableEnc: true}
	conn := newConn(fake, nil, newTestLogger(), proto{}, 0, false)
	// conn.unreliablePackets is intentionally left nil, as it would be for any Dialer/ListenConfig that
	// doesn't set UnreliablePackets.

	for _, pk := range testPacketSequence() {
		if err := conn.WritePacket(pk); err != nil {
			t.Fatalf("write packet: %v", err)
		}
	}
	if err := conn.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	reliable, unreliable := fake.batches()
	if len(unreliable) != 0 {
		t.Fatalf("expected no unreliable batches with a nil policy, got %d", len(unreliable))
	}
	if len(reliable) != 1 {
		t.Fatalf("expected a single flushed reliable batch, got %d", len(reliable))
	}
	if want := mustGoldenBytes(t); !bytes.Equal(reliable[0], want) {
		t.Fatalf("reliable batch diverged from golden v1.59.0 bytes:\ngot:  %x\nwant: %x", reliable[0], want)
	}
}

// TestWritePacket_PartitionsByPolicy verifies that once a transport implements packet.UnreliableWriter and
// disables encryption, and a non-nil UnreliablePackets policy selects a packet ID, WritePacket routes
// converted packets of that ID to the unreliable sink and everything else stays on the reliable sink.
// Decoding both batches and comparing exact ID sequences proves partitioning, exclusivity, and per-sink
// write order all at once.
func TestWritePacket_PartitionsByPolicy(t *testing.T) {
	fake := &fakeUnreliableConn{disableEnc: true}
	conn := newConn(fake, nil, newTestLogger(), proto{}, 0, false)
	conn.unreliablePackets = func(id uint32) bool { return id == packet.IDSetTime }

	if err := conn.WritePacket(&packet.SetTime{Time: 1}); err != nil {
		t.Fatalf("write packet: %v", err)
	}
	if err := conn.WritePacket(&packet.SetDifficulty{Difficulty: 9}); err != nil {
		t.Fatalf("write packet: %v", err)
	}
	if err := conn.WritePacket(&packet.SetTime{Time: 3}); err != nil {
		t.Fatalf("write packet: %v", err)
	}
	if err := conn.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	reliable, unreliable := fake.batches()
	if len(reliable) != 1 {
		t.Fatalf("expected exactly one reliable batch, got %d", len(reliable))
	}
	if len(unreliable) != 1 {
		t.Fatalf("expected exactly one unreliable batch, got %d", len(unreliable))
	}

	reliableIDs := decodeBatchIDs(t, reliable[0], nil)
	unreliableIDs := decodeBatchIDs(t, unreliable[0], nil)

	if want := []uint32{packet.IDSetDifficulty}; !slices.Equal(reliableIDs, want) {
		t.Fatalf("reliable batch decoded to unexpected IDs: got %v, want %v", reliableIDs, want)
	}
	// SetTime(1) before SetTime(3): write order preserved within the unreliable sink.
	if want := []uint32{packet.IDSetTime, packet.IDSetTime}; !slices.Equal(unreliableIDs, want) {
		t.Fatalf("unreliable batch decoded to unexpected IDs: got %v, want %v", unreliableIDs, want)
	}
}

// TestWrite_PartitionsByPolicy verifies that Write — the raw pre-serialised-bytes path used by callers such
// as a proxy forwarding packets it never decoded into a packet.Packet — partitions the same way WritePacket
// does: it peeks the packet ID from the raw header and routes to the unreliable sink when the policy selects
// it, leaving everything else reliable.
func TestWrite_PartitionsByPolicy(t *testing.T) {
	fake := &fakeUnreliableConn{disableEnc: true}
	conn := newConn(fake, nil, newTestLogger(), proto{}, 0, false)
	conn.unreliablePackets = func(id uint32) bool { return id == packet.IDSetTime }

	unreliableRaw := encodeRawPacket(conn, &packet.SetTime{Time: 1})
	reliableRaw := encodeRawPacket(conn, &packet.SetDifficulty{Difficulty: 9})

	if _, err := conn.Write(unreliableRaw); err != nil {
		t.Fatalf("write raw unreliable-ID packet: %v", err)
	}
	if _, err := conn.Write(reliableRaw); err != nil {
		t.Fatalf("write raw reliable-ID packet: %v", err)
	}
	if err := conn.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	reliable, unreliable := fake.batches()
	if len(reliable) != 1 {
		t.Fatalf("expected exactly one reliable batch, got %d", len(reliable))
	}
	if len(unreliable) != 1 {
		t.Fatalf("expected exactly one unreliable batch, got %d", len(unreliable))
	}

	if want := []uint32{packet.IDSetDifficulty}; !slices.Equal(decodeBatchIDs(t, reliable[0], nil), want) {
		t.Fatalf("reliable batch decoded to unexpected IDs: got %v, want %v", decodeBatchIDs(t, reliable[0], nil), want)
	}
	if want := []uint32{packet.IDSetTime}; !slices.Equal(decodeBatchIDs(t, unreliable[0], nil), want) {
		t.Fatalf("unreliable batch decoded to unexpected IDs: got %v, want %v", decodeBatchIDs(t, unreliable[0], nil), want)
	}
}

// TestWrite_MalformedHeaderFallsBackToReliable verifies that Write never errors or drops data when it can't
// make sense of b's header: it falls back to the reliable buffer, exactly as if no UnreliablePackets policy
// were configured at all.
func TestWrite_MalformedHeaderFallsBackToReliable(t *testing.T) {
	fake := &fakeUnreliableConn{disableEnc: true}
	conn := newConn(fake, nil, newTestLogger(), proto{}, 0, false)
	conn.unreliablePackets = func(uint32) bool { return true } // would route everything unreliable if it could read the ID

	malformed := [][]byte{
		nil,                            // empty
		{},                             // empty
		{0x80},                         // continuation bit set, no terminating byte
		{0x80, 0x80, 0x80, 0x80, 0x80}, // continuation bit set on all 5 bytes, never terminates
	}
	for _, b := range malformed {
		n, err := conn.Write(b)
		if err != nil {
			t.Fatalf("write %x: %v", b, err)
		}
		if n != len(b) {
			t.Fatalf("write %x: expected n=%d, got %d", b, len(b), n)
		}
	}
	if err := conn.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	reliable, unreliable := fake.batches()
	if len(unreliable) != 0 {
		t.Fatalf("expected no unreliable batches for malformed headers, got %d", len(unreliable))
	}
	if len(reliable) != 1 {
		t.Fatalf("expected a single flushed reliable batch, got %d", len(reliable))
	}
}

// TestUnreliableEnc_RequiresEncryptionDisabler verifies the precondition Conn enforces before it will ever
// build an unreliable encoder: the underlying transport must implement packet.EncryptionDisabler and have
// it report true. Minecraft encryption can't be kept in sync across two independent packet.Encoder
// instances, so a transport that leaves Minecraft encryption enabled must never get an unreliable sink,
// regardless of what UnreliablePackets selects.
func TestUnreliableEnc_RequiresEncryptionDisabler(t *testing.T) {
	assertAllReliable := func(t *testing.T, conn *Conn, reliable, unreliable [][]byte) {
		t.Helper()
		if conn.unreliableEnc != nil {
			t.Fatalf("expected no unreliable encoder to be built")
		}
		if len(unreliable) != 0 {
			t.Fatalf("expected no unreliable batches, got %d", len(unreliable))
		}
		if len(reliable) != 1 {
			t.Fatalf("expected the packet to go out reliably, got %d reliable batches", len(reliable))
		}
	}

	t.Run("transport does not implement EncryptionDisabler", func(t *testing.T) {
		fake := &unreliableConnNoEncryptionDisabler{}
		conn := newConn(fake, nil, newTestLogger(), proto{}, 0, false)
		conn.unreliablePackets = func(uint32) bool { return true }

		if err := conn.WritePacket(&packet.SetTime{Time: 5}); err != nil {
			t.Fatalf("write packet: %v", err)
		}
		if err := conn.Flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}
		reliable, unreliable := fake.batches()
		assertAllReliable(t, conn, reliable, unreliable)
	})

	t.Run("DisableEncryption reports false", func(t *testing.T) {
		fake := &fakeUnreliableConn{disableEnc: false}
		conn := newConn(fake, nil, newTestLogger(), proto{}, 0, false)
		conn.unreliablePackets = func(uint32) bool { return true }

		if err := conn.WritePacket(&packet.SetTime{Time: 5}); err != nil {
			t.Fatalf("write packet: %v", err)
		}
		if err := conn.Flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}
		reliable, unreliable := fake.batches()
		assertAllReliable(t, conn, reliable, unreliable)
	})
}

// TestWritePacket_RoundTripWithCompression exercises WritePacket/Flush and a raw Write after driving
// compression on through handleNetworkSettings, the real handler that enables it on a connection, with a
// policy set so both encoders are in play. It then decodes both resulting batches with a packet.Decoder
// configured for the same compression. This is what catches a missing unreliableEnc.EnableCompression call
// at that call site: an unreliable batch encoded without compression enabled fails to decode against a
// Decoder that expects it.
func TestWritePacket_RoundTripWithCompression(t *testing.T) {
	fake := &fakeUnreliableConn{disableEnc: true}
	conn := newConn(fake, nil, newTestLogger(), proto{}, 0, false)
	conn.unreliablePackets = func(id uint32) bool { return id == packet.IDSetTime }

	if err := conn.handleNetworkSettings(&packet.NetworkSettings{
		CompressionAlgorithm: packet.FlateCompression.EncodeCompression(),
		CompressionThreshold: 0,
	}); err != nil {
		t.Fatalf("handle NetworkSettings: %v", err)
	}

	if err := conn.WritePacket(&packet.SetTime{Time: 1}); err != nil {
		t.Fatalf("write packet: %v", err)
	}
	if err := conn.WritePacket(&packet.SetDifficulty{Difficulty: 9}); err != nil {
		t.Fatalf("write packet: %v", err)
	}
	// A raw Write of a pre-serialised unreliable-ID packet, matching a proxy that forwards packets it never
	// decoded, must partition and compress the same way a WritePacket call does.
	if _, err := conn.Write(encodeRawPacket(conn, &packet.SetTime{Time: 2})); err != nil {
		t.Fatalf("write raw unreliable-ID packet: %v", err)
	}
	if err := conn.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	reliable, unreliable := fake.batches()
	if len(reliable) != 1 || len(unreliable) != 1 {
		t.Fatalf("expected one reliable and one unreliable batch, got %d/%d", len(reliable), len(unreliable))
	}

	reliableIDs := decodeBatchIDs(t, reliable[0], packet.FlateCompression)
	unreliableIDs := decodeBatchIDs(t, unreliable[0], packet.FlateCompression)

	if want := []uint32{packet.IDSetDifficulty}; !slices.Equal(reliableIDs, want) {
		t.Fatalf("reliable batch decoded to unexpected IDs: got %v, want %v", reliableIDs, want)
	}
	if want := []uint32{packet.IDSetTime, packet.IDSetTime}; !slices.Equal(unreliableIDs, want) {
		t.Fatalf("unreliable batch decoded to unexpected IDs: got %v, want %v", unreliableIDs, want)
	}
}

// TestHandleRequestNetworkSettings_MirrorsCompression exercises handleRequestNetworkSettings, the
// server-side call site that enables compression during the RequestNetworkSettings/NetworkSettings
// handshake, and confirms it mirrors EnableCompression onto unreliableEnc the same way the client-side
// handleNetworkSettings does (see TestWritePacket_RoundTripWithCompression). Without the mirror, the
// unreliable batch below fails to decode against a Decoder that expects compression.
func TestHandleRequestNetworkSettings_MirrorsCompression(t *testing.T) {
	fake := &fakeUnreliableConn{disableEnc: true}
	conn := newConn(fake, nil, newTestLogger(), proto{}, 0, false)
	conn.acceptedProto = []Protocol{proto{}}
	conn.compression = packet.FlateCompression
	conn.unreliablePackets = func(id uint32) bool { return id == packet.IDSetTime }

	if err := conn.handleRequestNetworkSettings(&packet.RequestNetworkSettings{ClientProtocol: protocol.CurrentProtocol}); err != nil {
		t.Fatalf("handle RequestNetworkSettings: %v", err)
	}

	if err := conn.WritePacket(&packet.SetTime{Time: 1}); err != nil {
		t.Fatalf("write packet: %v", err)
	}
	if err := conn.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	_, unreliable := fake.batches()
	if len(unreliable) != 1 {
		t.Fatalf("expected exactly one unreliable batch, got %d", len(unreliable))
	}
	unreliableIDs := decodeBatchIDs(t, unreliable[0], packet.FlateCompression)
	if want := []uint32{packet.IDSetTime}; !slices.Equal(unreliableIDs, want) {
		t.Fatalf("unreliable batch decoded to unexpected IDs: got %v, want %v", unreliableIDs, want)
	}
}

// TestWritePacket_ConcurrentWriteAndFlush exercises WritePacket and Flush concurrently under -race, on a
// transport that implements packet.UnreliableWriter, disables encryption, and has a policy that routes some
// packets unreliably. It doesn't assert on the resulting bytes, only that the locking discipline around
// bufferedSend/bufferedSendUnreliable holds up without a data race or panic.
func TestWritePacket_ConcurrentWriteAndFlush(t *testing.T) {
	fake := &fakeUnreliableConn{disableEnc: true}
	conn := newConn(fake, nil, newTestLogger(), proto{}, 0, false)
	conn.unreliablePackets = func(id uint32) bool { return id == packet.IDSetTime }

	var wg sync.WaitGroup
	stop := make(chan struct{})

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			_ = conn.Flush()
		}
		close(stop)
	}()

	writer := func(pk func() packet.Packet) {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = conn.WritePacket(pk())
			}
		}
	}
	wg.Add(2)
	go writer(func() packet.Packet { return &packet.SetTime{Time: 1} })
	go writer(func() packet.Packet { return &packet.SetDifficulty{Difficulty: 1} })

	wg.Wait()
	_ = conn.Flush()
}
