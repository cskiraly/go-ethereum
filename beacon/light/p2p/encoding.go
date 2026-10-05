// Copyright 2026 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package p2p

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/golang/snappy"
)

// The consensus layer's `ssz_snappy` encoding: req/resp payloads are an unsigned varint
// with the SSZ length followed by the SSZ bytes in the snappy framing format; gossip
// messages are the SSZ bytes in the snappy block format.

// Size limits of the payloads this node reads (their SSZ length). The spec allows any
// payload up to MAX_PAYLOAD_SIZE (10 MiB); these are the sizes of the types read, with room
// to spare, so a peer can't make the node allocate or decompress more than they need.
const (
	maxStatusSize = 92       // StatusV2 (StatusV1: 84)
	maxPingSize   = 8        // ping, goodbye
	maxErrorSize  = 256      // ErrorMessage
	maxUpdateSize = 64 << 10 // LightClientUpdate, LightClientBootstrap (~27 KB)
	maxGossipSize = 8 << 10  // LightClientOptimisticUpdate and FinalityUpdate (~1 and ~2 KB)
)

var (
	domainValidSnappy   = [4]byte{0x01, 0x00, 0x00, 0x00}
	domainInvalidSnappy = [4]byte{0x00, 0x00, 0x00, 0x00}
)

// writeSSZ writes one ssz_snappy payload (length prefix + framed snappy).
func writeSSZ(w io.Writer, ssz []byte) error {
	var lenBuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lenBuf[:], uint64(len(ssz)))
	if _, err := w.Write(lenBuf[:n]); err != nil {
		return err
	}
	sw := snappy.NewBufferedWriter(w)
	if _, err := sw.Write(ssz); err != nil {
		return err
	}
	return sw.Close()
}

// readSSZ reads one ssz_snappy payload of at most max bytes from r, reading no more of
// the framed snappy data than a payload of its size takes.
func readSSZ(r *bufio.Reader, max int) ([]byte, error) {
	size, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, err
	}
	if size > uint64(max) {
		return nil, fmt.Errorf("payload too large: %d", size)
	}
	buf := make([]byte, size)
	if _, err := io.ReadFull(snappy.NewReader(io.LimitReader(r, maxFramedSize(int(size)))), buf); err != nil {
		return nil, fmt.Errorf("snappy: %v", err)
	}
	return buf, nil
}

// maxFramedSize bounds the snappy framing of n bytes as encoders write it: the stream
// identifier, and for each chunk (64 KiB, Teku 32 KiB) a header, a checksum and the data
// compressed by snappy at worst (snappy.MaxEncodedLen: 32 + n + n/6). Framing with tiny
// chunks or padding beyond that is cut off.
func maxFramedSize(n int) int64 {
	chunks := n/32768 + 1
	return int64(10 + chunks*(8+32) + n + n/6)
}

// writeResponse writes a successful response chunk, with context bytes if given.
func writeResponse(w io.Writer, context []byte, ssz []byte) error {
	if _, err := w.Write([]byte{0}); err != nil {
		return err
	}
	if len(context) > 0 {
		if _, err := w.Write(context); err != nil {
			return err
		}
	}
	return writeSSZ(w, ssz)
}

// readResponse reads a response chunk: result code, context bytes (contextLen of them),
// payload (at most max bytes). A non-zero result code is returned as an error with the
// error message.
func readResponse(r *bufio.Reader, contextLen int, max int) (context []byte, ssz []byte, err error) {
	code, err := r.ReadByte()
	if err != nil {
		return nil, nil, err
	}
	if code != 0 {
		msg, _ := readSSZ(r, maxErrorSize)
		return nil, nil, fmt.Errorf("response code %d: %q", code, msg)
	}
	if contextLen > 0 {
		context = make([]byte, contextLen)
		if _, err := io.ReadFull(r, context); err != nil {
			return nil, nil, err
		}
	}
	ssz, err = readSSZ(r, max)
	return context, ssz, err
}

// decodeGossip decompresses a gossip message (a light client update: at most
// maxGossipSize bytes).
func decodeGossip(data []byte) ([]byte, error) {
	n, err := snappy.DecodedLen(data)
	if err != nil {
		return nil, err
	}
	if n > maxGossipSize {
		return nil, errors.New("gossip message too large")
	}
	return snappy.Decode(nil, data)
}

// gossipMessageID is the Altair+ message-id: the first 20 bytes of
// SHA256(domain + uint64_le(len(topic)) + topic + data), with the decompressed data if
// it decompresses (a message too large to be a light client update counts as not
// decompressing: it is rejected anyway).
func gossipMessageID(topic string, data []byte) string {
	h := sha256.New()
	if dec, err := decodeGossip(data); err == nil {
		h.Write(domainValidSnappy[:])
		data = dec
	} else {
		h.Write(domainInvalidSnappy[:])
	}
	var l [8]byte
	binary.LittleEndian.PutUint64(l[:], uint64(len(topic)))
	h.Write(l[:])
	h.Write([]byte(topic))
	h.Write(data)
	return string(h.Sum(nil)[:20])
}
