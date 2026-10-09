// Copyright 2022 The go-ethereum Authors
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

package params

import (
	"crypto/sha256"
	"fmt"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/beacon/merkle"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/log"
	"gopkg.in/yaml.v3"
)

// syncCommitteeDomain specifies the signatures specific use to avoid clashes
// across signing different data structures.
const syncCommitteeDomain = 7

var knownForks = []string{"GENESIS", "ALTAIR", "BELLATRIX", "CAPELLA", "DENEB", "ELECTRA", "FULU"}

// ClientConfig contains beacon light client configuration.
type ClientConfig struct {
	ChainConfig
	Apis         []string
	CustomHeader map[string]string
	Threshold    int
	NoFilter     bool
}

// ChainConfig contains the beacon chain configuration.
type ChainConfig struct {
	GenesisTime           uint64      // Unix timestamp of slot 0
	GenesisValidatorsRoot common.Hash // Root hash of the genesis validator set, used for signature domain calculation
	Forks                 Forks
	Checkpoint            common.Hash
	CheckpointFile        string
}

// ForkAtEpoch returns the latest active fork at the given epoch.
func (c *ChainConfig) ForkAtEpoch(epoch uint64) Fork {
	for i := len(c.Forks) - 1; i >= 0; i-- {
		if c.Forks[i].Epoch <= epoch {
			return *c.Forks[i]
		}
	}
	return Fork{}
}

// AddFork adds a new item to the list of forks.
func (c *ChainConfig) AddFork(name string, epoch uint64, version []byte) *ChainConfig {
	knownIndex := slices.Index(knownForks, name)
	if knownIndex == -1 {
		knownIndex = math.MaxInt // assume that the unknown fork happens after the known ones
		if epoch != math.MaxUint64 {
			log.Warn("Unknown fork in config.yaml", "fork name", name, "known forks", knownForks)
		}
	}
	fork := &Fork{
		Name:       name,
		Epoch:      epoch,
		Version:    version,
		knownIndex: knownIndex,
	}
	fork.computeDomain(c.GenesisValidatorsRoot)
	c.Forks = append(c.Forks, fork)
	sort.Sort(c.Forks)
	return c
}

// LoadForks parses the beacon chain configuration file (config.yaml) and extracts
// the list of forks.
func (c *ChainConfig) LoadForks(file []byte) error {
	config := make(map[string]any)
	if err := yaml.Unmarshal(file, &config); err != nil {
		return fmt.Errorf("failed to parse beacon chain config file: %v", err)
	}
	var (
		versions = make(map[string][]byte)
		epochs   = make(map[string]uint64)
	)
	epochs["GENESIS"] = 0

	for key, value := range config {
		if value == nil {
			continue
		}
		if strings.HasSuffix(key, "_FORK_VERSION") {
			name := key[:len(key)-len("_FORK_VERSION")]
			switch version := value.(type) {
			case int:
				versions[name] = new(big.Int).SetUint64(uint64(version)).FillBytes(make([]byte, 4))
			case int64:
				versions[name] = new(big.Int).SetUint64(uint64(version)).FillBytes(make([]byte, 4))
			case uint64:
				versions[name] = new(big.Int).SetUint64(version).FillBytes(make([]byte, 4))
			case string:
				v, err := hexutil.Decode(version)
				if err != nil {
					return fmt.Errorf("failed to decode hex fork id %q in beacon chain config file: %v", version, err)
				}
				versions[name] = v
			default:
				return fmt.Errorf("invalid fork version %q in beacon chain config file", version)
			}
		}
		if strings.HasSuffix(key, "_FORK_EPOCH") {
			name := key[:len(key)-len("_FORK_EPOCH")]
			switch epoch := value.(type) {
			case int:
				epochs[name] = uint64(epoch)
			case int64:
				epochs[name] = uint64(epoch)
			case uint64:
				epochs[name] = epoch
			case string:
				v, err := strconv.ParseUint(epoch, 10, 64)
				if err != nil {
					return fmt.Errorf("failed to parse epoch number %q in beacon chain config file: %v", epoch, err)
				}
				epochs[name] = v
			default:
				return fmt.Errorf("invalid fork epoch %q in beacon chain config file", epoch)
			}
		}
	}
	for name, epoch := range epochs {
		if version, ok := versions[name]; ok {
			delete(versions, name)
			c.AddFork(name, epoch, version)
		} else {
			return fmt.Errorf("fork id missing for %q in beacon chain config file", name)
		}
	}
	for name := range versions {
		return fmt.Errorf("epoch number missing for fork %q in beacon chain config file", name)
	}
	return nil
}

// Fork describes a single beacon chain fork and also stores the calculated
// signature domain used after this fork.
type Fork struct {
	// Name of the fork in the chain config (config.yaml) file
	Name string

	// Epoch when given fork version is activated
	Epoch uint64

	// Fork version, see https://github.com/ethereum/consensus-specs/blob/dev/specs/phase0/beacon-chain.md#custom-types
	Version []byte

	// index in list of known forks or MaxInt if unknown
	knownIndex int

	// calculated by computeDomain, based on fork version and genesis validators root
	domain merkle.Value
}

// computeDomain returns the signature domain based on the given fork version
// and genesis validator set root.
func (f *Fork) computeDomain(genesisValidatorsRoot common.Hash) {
	var (
		hasher        = sha256.New()
		forkVersion32 merkle.Value
		forkDataRoot  merkle.Value
	)
	copy(forkVersion32[:], f.Version)
	hasher.Write(forkVersion32[:])
	hasher.Write(genesisValidatorsRoot[:])
	hasher.Sum(forkDataRoot[:0])

	f.domain[0] = syncCommitteeDomain
	copy(f.domain[4:], forkDataRoot[:28])
}

// Forks is the list of all beacon chain forks in the chain configuration.
type Forks []*Fork

// domain returns the signature domain for the given epoch (assumes that domains
// have already been calculated).
func (f Forks) domain(epoch uint64) (merkle.Value, error) {
	for i := len(f) - 1; i >= 0; i-- {
		if epoch >= f[i].Epoch {
			return f[i].domain, nil
		}
	}
	return merkle.Value{}, fmt.Errorf("unknown fork for epoch %d", epoch)
}

// SigningRoot calculates the signing root of the given header.
func (f Forks) SigningRoot(epoch uint64, root common.Hash) (common.Hash, error) {
	domain, err := f.domain(epoch)
	if err != nil {
		return common.Hash{}, err
	}
	var (
		signingRoot common.Hash
		hasher      = sha256.New()
	)
	hasher.Write(root[:])
	hasher.Write(domain[:])
	hasher.Sum(signingRoot[:0])

	return signingRoot, nil
}

func (f Forks) Len() int      { return len(f) }
func (f Forks) Swap(i, j int) { f[i], f[j] = f[j], f[i] }
func (f Forks) Less(i, j int) bool {
	if f[i].Epoch != f[j].Epoch {
		return f[i].Epoch < f[j].Epoch
	}
	return f[i].knownIndex < f[j].knownIndex
}

// SetCheckpointFile sets the checkpoint import/export file name and attempts to
// read the checkpoint from the file if it already exists. It returns true if
// a checkpoint has been loaded. An empty file is ignored, like a missing one.
func (c *ChainConfig) SetCheckpointFile(checkpointFile string) (bool, error) {
	c.CheckpointFile = checkpointFile
	file, err := os.ReadFile(checkpointFile)
	if os.IsNotExist(err) {
		return false, nil // did not load checkpoint
	}
	if err != nil {
		return false, fmt.Errorf("failed to read beacon checkpoint file: %v", err)
	}
	if len(file) == 0 {
		// A crash while the file was written in place can leave it empty, and
		// an empty file may also have been created ahead of the first start.
		log.Warn("Ignoring empty beacon checkpoint file", "file", checkpointFile)
		return false, nil
	}
	cp, err := hexutil.Decode(string(file))
	if err != nil {
		return false, fmt.Errorf("failed to decode hex string in beacon checkpoint file: %v", err)
	}
	if len(cp) != 32 {
		return false, fmt.Errorf("invalid hex string length in beacon checkpoint file: %d", len(cp))
	}
	copy(c.Checkpoint[:len(cp)], cp)
	return true, nil
}

// SaveCheckpointToFile saves the given checkpoint to file if a checkpoint
// import/export file has been specified.
func (c *ChainConfig) SaveCheckpointToFile(checkpoint common.Hash) (bool, error) {
	if c.CheckpointFile == "" {
		return false, nil
	}
	data := []byte(checkpoint.Hex())
	if err := replaceFile(c.CheckpointFile, data); err != nil {
		// The file can't be replaced where it is a mount point itself, or where
		// its directory isn't writable: write it in place there. A crash during
		// the write can leave it empty, which SetCheckpointFile ignores.
		log.Debug("Writing beacon checkpoint file in place", "file", c.CheckpointFile, "err", err)
		if err := writeFileSync(c.CheckpointFile, data, 0600); err != nil {
			return false, fmt.Errorf("failed to write beacon checkpoint file: %v", err)
		}
	}
	return true, nil
}

// replaceFile replaces the named file, or the file a symbolic link points to,
// with a new one holding data and having the same permissions. The new file is
// written and synced next to the old one first, so that a crash leaves either
// the old or the new content. The directory isn't synced: after a crash, the
// old checkpoint is still a valid one.
func replaceFile(name string, data []byte) error {
	if target, err := filepath.EvalSymlinks(name); err == nil {
		name = target
	}
	perm := os.FileMode(0600)
	if info, err := os.Stat(name); err == nil {
		perm = info.Mode().Perm()
	}
	tmp := name + ".tmp"
	err := writeFileSync(tmp, data, perm)
	if err == nil {
		err = os.Chmod(tmp, perm) // the umask may have changed it, or a stale file has its own
	}
	if err == nil {
		err = os.Rename(tmp, name)
	}
	if err != nil {
		os.Remove(tmp)
	}
	return err
}

// writeFileSync is like os.WriteFile, but syncs the file before closing it.
func writeFileSync(name string, data []byte, perm os.FileMode) error {
	f, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	if err1 := f.Close(); err1 != nil && err == nil {
		err = err1
	}
	return err
}
