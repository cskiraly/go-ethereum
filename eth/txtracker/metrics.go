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

package txtracker

import "github.com/ethereum/go-ethereum/metrics"

var (
	txTrackedMeter   = metrics.NewRegisteredMeter("eth/txtracker/tracked", nil)
	txAnnouncedMeter       = metrics.NewRegisteredMeter("eth/txtracker/announced", nil)
	txFetchRequestedMeter  = metrics.NewRegisteredMeter("eth/txtracker/fetch_requested", nil)
	txReceivedMeter        = metrics.NewRegisteredMeter("eth/txtracker/received", nil)
	txPooledMeter    = metrics.NewRegisteredMeter("eth/txtracker/pooled", nil)
	txIncludedMeter              = metrics.NewRegisteredMeter("eth/txtracker/included", nil)
	txIncludedFromAnnouncedMeter = metrics.NewRegisteredMeter("eth/txtracker/included/from_announced", nil)
	txIncludedFromReceivedMeter  = metrics.NewRegisteredMeter("eth/txtracker/included/from_received", nil)
	txIncludedFromPooledMeter    = metrics.NewRegisteredMeter("eth/txtracker/included/from_pooled", nil)
	txIncludedFromRejectedMeter  = metrics.NewRegisteredMeter("eth/txtracker/included/from_rejected", nil)
	txFinalizedMeter            = metrics.NewRegisteredMeter("eth/txtracker/finalized", nil)
	txRejectedMeter             = metrics.NewRegisteredMeter("eth/txtracker/rejected", nil)
	txDroppedMeter              = metrics.NewRegisteredMeter("eth/txtracker/dropped", nil)
	txReaddedMeter              = metrics.NewRegisteredMeter("eth/txtracker/readded", nil)
	txIncludedFromDroppedMeter  = metrics.NewRegisteredMeter("eth/txtracker/included/from_dropped", nil)
	txReorgedMeter              = metrics.NewRegisteredMeter("eth/txtracker/reorged", nil)
	txChainOnlyMeter = metrics.NewRegisteredMeter("eth/txtracker/chainonly", nil)
	txLocalMeter     = metrics.NewRegisteredMeter("eth/txtracker/local", nil)
	txEvictedMeter      = metrics.NewRegisteredMeter("eth/txtracker/evicted", nil)
	txEmitDroppedMeter  = metrics.NewRegisteredMeter("eth/txtracker/emit_dropped", nil)
	txTrackerSize       = metrics.NewRegisteredGauge("eth/txtracker/size", nil)
)
