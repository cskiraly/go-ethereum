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

// Bouncing-protection metrics. The cleared/gateBlocked pair quantify
// the stage-2 fee gate's effect:
//
//	gate_save_rate = gate_blocked / (cleared + gate_blocked)
//
// — the fraction of sender-inclusion clearances the gate kept blocked
// because the stored fee was below the current pool floor (a refetch
// would have just bounced off as underpriced).
//
// The size gauge tracks the current bouncing-map size — a sanity
// check on growth bounded by tracker FIFO capacity.
var (
	bouncingClearedMeter        = metrics.NewRegisteredMeter("eth/txtracker/bouncing/cleared", nil)
	bouncingGateBlockedMeter    = metrics.NewRegisteredMeter("eth/txtracker/bouncing/gate_blocked", nil)
	bouncingSizeGauge           = metrics.NewRegisteredGauge("eth/txtracker/bouncing/size", nil)
	bouncingInsertedDropMeter   = metrics.NewRegisteredMeter("eth/txtracker/bouncing/inserted/drop", nil)
	bouncingInsertedRejectMeter = metrics.NewRegisteredMeter("eth/txtracker/bouncing/inserted/reject", nil)
)
