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
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/attestantio/go-eth2-client/spec/deneb"
	"github.com/ethereum/go-ethereum/beacon/merkle"
	"github.com/ethereum/go-ethereum/beacon/params"
	"github.com/ethereum/go-ethereum/beacon/types"
	"github.com/ethereum/go-ethereum/common"
)

// SSZ layouts of the light client messages, for the forks from Deneb up to Fulu
// (their LightClientHeader carries a Deneb execution payload header).
const (
	beaconHeaderSize    = 8 + 8 + 32 + 32 + 32
	execBranchDepth     = 4
	lcHeaderFixedSize   = beaconHeaderSize + 4 + execBranchDepth*32
	syncAggregateSize   = params.SyncCommitteeBitmaskSize + params.BLSSignatureSize
	optimisticFixedSize = 4 + syncAggregateSize + 8
)

var errShort = errors.New("ssz: input too short")

// finalityBranchDepth is the depth of the finalized checkpoint proof in a fork's state.
func finalityBranchDepth(fork string) int {
	switch fork {
	case "altair", "bellatrix", "capella", "deneb":
		return 6
	default:
		return 7 // electra, fulu
	}
}

// decodable reports whether the light client messages of a fork are decoded here.
func decodable(fork string) bool {
	return fork == "gloas" || supportedFork(fork)
}

// supportedFork reports whether the light client messages of a fork are decoded with the
// Deneb layout of the light client header.
func supportedFork(fork string) bool {
	switch fork {
	case "deneb", "electra", "fulu":
		return true
	}
	return false
}

func decodeBeaconHeader(b []byte) types.Header {
	var h types.Header
	h.Slot = binary.LittleEndian.Uint64(b[0:8])
	h.ProposerIndex = binary.LittleEndian.Uint64(b[8:16])
	copy(h.ParentRoot[:], b[16:48])
	copy(h.StateRoot[:], b[48:80])
	copy(h.BodyRoot[:], b[80:112])
	return h
}

func decodeBranch(b []byte, depth int) merkle.Values {
	v := make(merkle.Values, depth)
	for i := range v {
		copy(v[i][:], b[i*32:(i+1)*32])
	}
	return v
}

// decodeLCHeader decodes a LightClientHeader (Deneb layout) into a header with its
// execution payload header proof.
func decodeLCHeader(b []byte) (types.HeaderWithExecProof, error) {
	if len(b) < lcHeaderFixedSize {
		return types.HeaderWithExecProof{}, errShort
	}
	header := decodeBeaconHeader(b[:beaconHeaderSize])
	off := binary.LittleEndian.Uint32(b[beaconHeaderSize : beaconHeaderSize+4])
	if int(off) != lcHeaderFixedSize {
		return types.HeaderWithExecProof{}, fmt.Errorf("ssz: bad execution header offset %d", off)
	}
	branch := decodeBranch(b[beaconHeaderSize+4:lcHeaderFixedSize], execBranchDepth)
	payload := new(deneb.ExecutionPayloadHeader)
	if err := payload.UnmarshalSSZ(b[lcHeaderFixedSize:]); err != nil {
		return types.HeaderWithExecProof{}, fmt.Errorf("ssz: execution header: %v", err)
	}
	return types.HeaderWithExecProof{
		Header: header,
		Proof:  &types.LegacyHeaderProof{PayloadHeader: types.NewExecutionHeader(payload), Branch: branch},
	}, nil
}

func decodeSyncAggregate(b []byte) types.SyncAggregate {
	var s types.SyncAggregate
	copy(s.Signers[:], b[:params.SyncCommitteeBitmaskSize])
	copy(s.Signature[:], b[params.SyncCommitteeBitmaskSize:syncAggregateSize])
	return s
}

// DecodeOptimisticUpdate decodes an SSZ LightClientOptimisticUpdate of the given fork.
func DecodeOptimisticUpdate(fork string, b []byte) (types.OptimisticUpdate, error) {
	if fork == "gloas" {
		return decodeGloasOptimistic(b)
	}
	if !supportedFork(fork) {
		return types.OptimisticUpdate{}, fmt.Errorf("light client updates of fork %q are not supported", fork)
	}
	if len(b) < optimisticFixedSize {
		return types.OptimisticUpdate{}, errShort
	}
	if off := binary.LittleEndian.Uint32(b[0:4]); int(off) != optimisticFixedSize {
		return types.OptimisticUpdate{}, fmt.Errorf("ssz: bad attested header offset %d", off)
	}
	attested, err := decodeLCHeader(b[optimisticFixedSize:])
	if err != nil {
		return types.OptimisticUpdate{}, err
	}
	return types.OptimisticUpdate{
		Attested:      attested,
		Signature:     decodeSyncAggregate(b[4 : 4+syncAggregateSize]),
		SignatureSlot: binary.LittleEndian.Uint64(b[4+syncAggregateSize : optimisticFixedSize]),
	}, nil
}

// DecodeFinalityUpdate decodes an SSZ LightClientFinalityUpdate of the given fork.
func DecodeFinalityUpdate(fork string, b []byte) (types.FinalityUpdate, error) {
	if fork == "gloas" {
		return decodeGloasFinality(b)
	}
	if !supportedFork(fork) {
		return types.FinalityUpdate{}, fmt.Errorf("light client updates of fork %q are not supported", fork)
	}
	depth := finalityBranchDepth(fork)
	fixed := 4 + 4 + depth*32 + syncAggregateSize + 8
	if len(b) < fixed {
		return types.FinalityUpdate{}, errShort
	}
	offA, offF := binary.LittleEndian.Uint32(b[0:4]), binary.LittleEndian.Uint32(b[4:8])
	if int(offA) != fixed || offF < offA || uint64(offF) > uint64(len(b)) {
		return types.FinalityUpdate{}, fmt.Errorf("ssz: bad offsets %d, %d", offA, offF)
	}
	attested, err := decodeLCHeader(b[offA:offF])
	if err != nil {
		return types.FinalityUpdate{}, err
	}
	finalized, err := decodeLCHeader(b[offF:])
	if err != nil {
		return types.FinalityUpdate{}, err
	}
	p := 8 + depth*32
	return types.FinalityUpdate{
		Version:        fork,
		Attested:       attested,
		Finalized:      finalized,
		FinalityBranch: decodeBranch(b[8:p], depth),
		Signature:      decodeSyncAggregate(b[p : p+syncAggregateSize]),
		SignatureSlot:  binary.LittleEndian.Uint64(b[p+syncAggregateSize : fixed]),
	}, nil
}

// execHash returns the execution block hash proven by a header, or zero.
func execHash(h types.HeaderWithExecProof) common.Hash { return h.BlockHash() }

// committeeBranchDepth is the depth of the (next) sync committee proof in a fork's state.
func committeeBranchDepth(fork string) int {
	switch fork {
	case "altair", "bellatrix", "capella", "deneb":
		return 5
	default:
		return 6 // electra, fulu
	}
}

// DecodeUpdate decodes an SSZ LightClientUpdate of the given fork, and returns it
// with the next sync committee it announces.
func DecodeUpdate(fork string, b []byte) (*types.LightClientUpdate, *types.SerializedSyncCommittee, error) {
	if fork == "gloas" {
		return decodeGloasUpdate(b)
	}
	if !supportedFork(fork) {
		return nil, nil, fmt.Errorf("light client updates of fork %q are not supported", fork)
	}
	cDepth, fDepth := committeeBranchDepth(fork), finalityBranchDepth(fork)
	var (
		pCommittee = 4
		pCBranch   = pCommittee + types.SerializedSyncCommitteeSize
		pOffFin    = pCBranch + cDepth*32
		pFBranch   = pOffFin + 4
		pAggregate = pFBranch + fDepth*32
		pSigSlot   = pAggregate + syncAggregateSize
		fixed      = pSigSlot + 8
	)
	if len(b) < fixed {
		return nil, nil, errShort
	}
	offA, offF := binary.LittleEndian.Uint32(b[0:4]), binary.LittleEndian.Uint32(b[pOffFin:pFBranch])
	if int(offA) != fixed || offF < offA || uint64(offF) > uint64(len(b)) {
		return nil, nil, fmt.Errorf("ssz: bad offsets %d, %d", offA, offF)
	}
	attested, err := decodeLCHeader(b[offA:offF])
	if err != nil {
		return nil, nil, err
	}
	finalized, err := decodeLCHeader(b[offF:])
	if err != nil {
		return nil, nil, err
	}
	committee := new(types.SerializedSyncCommittee)
	copy(committee[:], b[pCommittee:pCBranch])
	update := &types.LightClientUpdate{
		Version: fork,
		AttestedHeader: types.SignedHeader{
			Header:        attested.Header,
			Signature:     decodeSyncAggregate(b[pAggregate:pSigSlot]),
			SignatureSlot: binary.LittleEndian.Uint64(b[pSigSlot:fixed]),
		},
		NextSyncCommitteeRoot:   committee.Root(),
		NextSyncCommitteeBranch: decodeBranch(b[pCBranch:pOffFin], cDepth),
		FinalityBranch:          decodeBranch(b[pFBranch:pAggregate], fDepth),
	}
	if finalized.Header != (types.Header{}) {
		fh := finalized.Header
		update.FinalizedHeader = &fh
	}
	return update, committee, nil
}

// DecodeBootstrap decodes an SSZ LightClientBootstrap of the given fork.
func DecodeBootstrap(fork string, b []byte) (*types.BootstrapData, error) {
	if fork == "gloas" {
		return decodeGloasBootstrap(b)
	}
	if !supportedFork(fork) {
		return nil, fmt.Errorf("light client bootstrap of fork %q is not supported", fork)
	}
	depth := committeeBranchDepth(fork)
	fixed := 4 + types.SerializedSyncCommitteeSize + depth*32
	if len(b) < fixed {
		return nil, errShort
	}
	if off := binary.LittleEndian.Uint32(b[0:4]); int(off) != fixed {
		return nil, fmt.Errorf("ssz: bad header offset %d", off)
	}
	header, err := decodeLCHeader(b[fixed:])
	if err != nil {
		return nil, err
	}
	committee := new(types.SerializedSyncCommittee)
	copy(committee[:], b[4:4+types.SerializedSyncCommitteeSize])
	return &types.BootstrapData{
		Version:         fork,
		Header:          header.Header,
		CommitteeRoot:   committee.Root(),
		Committee:       committee,
		CommitteeBranch: decodeBranch(b[4+types.SerializedSyncCommitteeSize:fixed], depth),
	}, nil
}

// Gloas: the LightClientHeader carries only the execution block hash (EIP-7732), and the
// state proofs are deeper (EIP-7688). All its containers are fixed-size then: no offsets.
const (
	gloasExecDepth      = 11 // floorlog2(EXECUTION_BLOCK_HASH_GINDEX_GLOAS)
	gloasFinalityDepth  = 9  // floorlog2(FINALIZED_ROOT_GINDEX_GLOAS)
	gloasCommitteeDepth = 11 // floorlog2((NEXT|CURRENT)_SYNC_COMMITTEE_GINDEX_GLOAS)
	gloasHeaderSize     = beaconHeaderSize + 32 + gloasExecDepth*32
)

func decodeGloasHeader(b []byte) types.HeaderWithExecProof {
	var hash common.Hash
	copy(hash[:], b[beaconHeaderSize:beaconHeaderSize+32])
	return types.HeaderWithExecProof{
		Header: decodeBeaconHeader(b[:beaconHeaderSize]),
		Proof: &types.GloasExecutionProof{
			ExecutionBlockHash: hash,
			Branch:             decodeBranch(b[beaconHeaderSize+32:gloasHeaderSize], gloasExecDepth),
		},
	}
}

func decodeGloasOptimistic(b []byte) (types.OptimisticUpdate, error) {
	if len(b) != gloasHeaderSize+syncAggregateSize+8 {
		return types.OptimisticUpdate{}, fmt.Errorf("ssz: gloas optimistic update of %d bytes", len(b))
	}
	p := gloasHeaderSize
	return types.OptimisticUpdate{
		Attested:      decodeGloasHeader(b),
		Signature:     decodeSyncAggregate(b[p : p+syncAggregateSize]),
		SignatureSlot: binary.LittleEndian.Uint64(b[p+syncAggregateSize:]),
	}, nil
}

func decodeGloasFinality(b []byte) (types.FinalityUpdate, error) {
	pBranch := 2 * gloasHeaderSize
	pAgg := pBranch + gloasFinalityDepth*32
	if len(b) != pAgg+syncAggregateSize+8 {
		return types.FinalityUpdate{}, fmt.Errorf("ssz: gloas finality update of %d bytes", len(b))
	}
	return types.FinalityUpdate{
		Version:        "gloas",
		Attested:       decodeGloasHeader(b),
		Finalized:      decodeGloasHeader(b[gloasHeaderSize:]),
		FinalityBranch: decodeBranch(b[pBranch:pAgg], gloasFinalityDepth),
		Signature:      decodeSyncAggregate(b[pAgg : pAgg+syncAggregateSize]),
		SignatureSlot:  binary.LittleEndian.Uint64(b[pAgg+syncAggregateSize:]),
	}, nil
}

func decodeGloasUpdate(b []byte) (*types.LightClientUpdate, *types.SerializedSyncCommittee, error) {
	var (
		pCommittee = gloasHeaderSize
		pCBranch   = pCommittee + types.SerializedSyncCommitteeSize
		pFinalized = pCBranch + gloasCommitteeDepth*32
		pFBranch   = pFinalized + gloasHeaderSize
		pAgg       = pFBranch + gloasFinalityDepth*32
		size       = pAgg + syncAggregateSize + 8
	)
	if len(b) != size {
		return nil, nil, fmt.Errorf("ssz: gloas update of %d bytes, want %d", len(b), size)
	}
	committee := new(types.SerializedSyncCommittee)
	copy(committee[:], b[pCommittee:pCBranch])
	update := &types.LightClientUpdate{
		Version: "gloas",
		AttestedHeader: types.SignedHeader{
			Header:        decodeBeaconHeader(b[:beaconHeaderSize]),
			Signature:     decodeSyncAggregate(b[pAgg : pAgg+syncAggregateSize]),
			SignatureSlot: binary.LittleEndian.Uint64(b[pAgg+syncAggregateSize:]),
		},
		NextSyncCommitteeRoot:   committee.Root(),
		NextSyncCommitteeBranch: decodeBranch(b[pCBranch:pFinalized], gloasCommitteeDepth),
		FinalityBranch:          decodeBranch(b[pFBranch:pAgg], gloasFinalityDepth),
	}
	if fh := decodeBeaconHeader(b[pFinalized : pFinalized+beaconHeaderSize]); fh != (types.Header{}) {
		update.FinalizedHeader = &fh
	}
	return update, committee, nil
}

func decodeGloasBootstrap(b []byte) (*types.BootstrapData, error) {
	size := gloasHeaderSize + types.SerializedSyncCommitteeSize + gloasCommitteeDepth*32
	if len(b) != size {
		return nil, fmt.Errorf("ssz: gloas bootstrap of %d bytes, want %d", len(b), size)
	}
	committee := new(types.SerializedSyncCommittee)
	copy(committee[:], b[gloasHeaderSize:gloasHeaderSize+types.SerializedSyncCommitteeSize])
	return &types.BootstrapData{
		Version:         "gloas",
		Header:          decodeBeaconHeader(b[:beaconHeaderSize]),
		CommitteeRoot:   committee.Root(),
		Committee:       committee,
		CommitteeBranch: decodeBranch(b[gloasHeaderSize+types.SerializedSyncCommitteeSize:], gloasCommitteeDepth),
	}, nil
}
