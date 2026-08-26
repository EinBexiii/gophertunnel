package packet

import (
	"bytes"
	"testing"
)

// bareUnreliableConn is a transport double implementing only io.Writer and UnreliableWriter, writing to
// two independent buffers so reliable and unreliable output can be compared directly. It deliberately does
// not implement BatchHeaderer or EncryptionDisabler, matching a transport that relies on Encoder defaults.
type bareUnreliableConn struct {
	reliable   bytes.Buffer
	unreliable bytes.Buffer
}

func (f *bareUnreliableConn) Write(b []byte) (int, error) {
	return f.reliable.Write(b)
}

func (f *bareUnreliableConn) WriteUnreliable(b []byte) (int, error) {
	return f.unreliable.Write(b)
}

// framedDisabledConn extends bareUnreliableConn with BatchHeaderer and EncryptionDisabler, mirroring a
// transport, such as NetherNet, that frames its own batches and disables Minecraft encryption.
type framedDisabledConn struct {
	bareUnreliableConn
	header []byte
}

func (f *framedDisabledConn) BatchHeader() []byte {
	return f.header
}

func (f *framedDisabledConn) DisableEncryption() bool {
	return true
}

func TestNewUnreliableEncoder_DefaultsMatchNewEncoder(t *testing.T) {
	// A transport that only implements UnreliableWriter (no BatchHeaderer, no EncryptionDisabler) must
	// produce the exact same framing as a transport used directly with NewEncoder.
	f := &bareUnreliableConn{}
	reliable := NewEncoder(f)
	unreliable := NewUnreliableEncoder(f)

	batch := [][]byte{[]byte("hello"), []byte("world")}
	if err := reliable.Encode(batch); err != nil {
		t.Fatalf("encode reliable: %v", err)
	}
	if err := unreliable.Encode(batch); err != nil {
		t.Fatalf("encode unreliable: %v", err)
	}

	if !bytes.Equal(f.reliable.Bytes(), f.unreliable.Bytes()) {
		t.Fatalf("reliable and unreliable output diverged:\nreliable:   %x\nunreliable: %x", f.reliable.Bytes(), f.unreliable.Bytes())
	}
	if len(f.reliable.Bytes()) == 0 || f.reliable.Bytes()[0] != header {
		t.Fatalf("expected default batch header 0x%x, got %x", header, f.reliable.Bytes())
	}
}

func TestNewUnreliableEncoder_ForwardsBatchHeaderer(t *testing.T) {
	f := &framedDisabledConn{header: nil}
	reliable := NewEncoder(f)
	unreliable := NewUnreliableEncoder(f)

	batch := [][]byte{[]byte("payload")}
	if err := reliable.Encode(batch); err != nil {
		t.Fatalf("encode reliable: %v", err)
	}
	if err := unreliable.Encode(batch); err != nil {
		t.Fatalf("encode unreliable: %v", err)
	}

	if !bytes.Equal(f.reliable.Bytes(), f.unreliable.Bytes()) {
		t.Fatalf("reliable and unreliable output diverged despite identical BatchHeaderer:\nreliable:   %x\nunreliable: %x", f.reliable.Bytes(), f.unreliable.Bytes())
	}
	if bytes.HasPrefix(f.reliable.Bytes(), []byte{header}) {
		t.Fatalf("expected the transport-supplied nil header, not the package default: %x", f.reliable.Bytes())
	}
}

func TestNewUnreliableEncoder_ForwardsEncryptionDisabler(t *testing.T) {
	f := &framedDisabledConn{}
	reliable := NewEncoder(f)
	unreliable := NewUnreliableEncoder(f)

	var key [32]byte
	for i := range key {
		key[i] = byte(i)
	}
	reliable.EnableEncryption(key)
	unreliable.EnableEncryption(key)

	batch := [][]byte{[]byte("plaintext-should-stay-plaintext")}
	if err := reliable.Encode(batch); err != nil {
		t.Fatalf("encode reliable: %v", err)
	}
	if err := unreliable.Encode(batch); err != nil {
		t.Fatalf("encode unreliable: %v", err)
	}

	if !bytes.Equal(f.reliable.Bytes(), f.unreliable.Bytes()) {
		t.Fatalf("reliable and unreliable output diverged on encryption-disabled state:\nreliable:   %x\nunreliable: %x", f.reliable.Bytes(), f.unreliable.Bytes())
	}
	if !bytes.Contains(f.reliable.Bytes(), []byte("plaintext-should-stay-plaintext")) {
		t.Fatalf("expected encryption to remain disabled, payload was encrypted: %x", f.reliable.Bytes())
	}
}

func TestNewUnreliableEncoder_MirrorsCompression(t *testing.T) {
	f := &bareUnreliableConn{}
	reliable := NewEncoder(f)
	unreliable := NewUnreliableEncoder(f)

	reliable.EnableCompression(FlateCompression, 1)
	unreliable.EnableCompression(FlateCompression, 1)

	batch := [][]byte{bytes.Repeat([]byte("a"), 512)}
	if err := reliable.Encode(batch); err != nil {
		t.Fatalf("encode reliable: %v", err)
	}
	if err := unreliable.Encode(batch); err != nil {
		t.Fatalf("encode unreliable: %v", err)
	}

	if !bytes.Equal(f.reliable.Bytes(), f.unreliable.Bytes()) {
		t.Fatalf("reliable and unreliable output diverged under compression:\nreliable:   %x\nunreliable: %x", f.reliable.Bytes(), f.unreliable.Bytes())
	}
}
