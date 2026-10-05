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
	"crypto/ecdsa"
	"errors"
	"os"
	"path/filepath"

	"github.com/ethereum/go-ethereum/crypto"
)

// LoadOrCreateKey loads the node key from file, creating the file if it doesn't exist yet
// (as geth does its nodekey), so that the node keeps its identity, and the reputation
// peers keep for it, across restarts.
func LoadOrCreateKey(file string) (*ecdsa.PrivateKey, error) {
	key, err := crypto.LoadECDSA(file)
	if err == nil || !errors.Is(err, os.ErrNotExist) {
		return key, err
	}
	if key, err = crypto.GenerateKey(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(file), 0700); err != nil {
		return nil, err
	}
	return key, crypto.SaveECDSA(file, key)
}
