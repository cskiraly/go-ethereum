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

const maxChunkSize = 10 << 20 // MAX_PAYLOAD_SIZE

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

// readSSZ reads one ssz_snappy payload from r.
func readSSZ(r *bufio.Reader) ([]byte, error) {
	size, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, err
	}
	if size > maxChunkSize {
		return nil, fmt.Errorf("payload too large: %d", size)
	}
	buf := make([]byte, size)
	if _, err := io.ReadFull(snappy.NewReader(r), buf); err != nil {
		return nil, fmt.Errorf("snappy: %v", err)
	}
	return buf, nil
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
// payload. A non-zero result code is returned as an error with the error message.
func readResponse(r *bufio.Reader, contextLen int) (context []byte, ssz []byte, err error) {
	code, err := r.ReadByte()
	if err != nil {
		return nil, nil, err
	}
	if code != 0 {
		msg, _ := readSSZ(r)
		return nil, nil, fmt.Errorf("response code %d: %q", code, msg)
	}
	if contextLen > 0 {
		context = make([]byte, contextLen)
		if _, err := io.ReadFull(r, context); err != nil {
			return nil, nil, err
		}
	}
	ssz, err = readSSZ(r)
	return context, ssz, err
}

// decodeGossip decompresses a gossip message.
func decodeGossip(data []byte) ([]byte, error) {
	n, err := snappy.DecodedLen(data)
	if err != nil {
		return nil, err
	}
	if n > maxChunkSize {
		return nil, errors.New("gossip message too large")
	}
	return snappy.Decode(nil, data)
}

// gossipMessageID is the Altair+ message-id: the first 20 bytes of
// SHA256(domain + uint64_le(len(topic)) + topic + data), with the decompressed data if
// it decompresses.
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
