package parallel

import (
	"maps"
	"time"
)

// CarryStats counts what Carry did with the results it was given.
type CarryStats struct {
	Carried  int // results builds on the new head can start from
	Refused  int // results that read what changed, or whose basis did: predictions now
	Unusable int // failed results and predictions, carried as they are
	Time     time.Duration
}

// Carry returns the results of a Preexecutor on an earlier head for builds on
// this one's head, in the same order, which must have every result after its
// basis. A result stays usable if what it read from its parent state is the
// same in this one: the transactions in between changed none of it. Its
// basis keeps the earlier transactions of its sender still pending; one
// included in between drops out, and what it wrote must be what this state
// holds. A result that read anything else that changed becomes a prediction.
// The header values and system call writes it read are left to the build
// (Engine.adapt), which knows its header.
func (p *Preexecutor) Carry(results []*Result) ([]*Result, CarryStats) {
	var stats CarryStats
	started := time.Now()
	parent := newVersionedStateReader(noVersions, newStore(), p.exec.parent)
	carried := make(map[*Result]*Result, len(results))
	out := make([]*Result, len(results))
	for i, r := range results {
		if r == nil {
			continue
		}
		c := p.carry(r, carried, parent)
		switch {
		case !r.Usable():
			stats.Unusable++
		case c.Usable():
			stats.Carried++
		default:
			stats.Refused++
		}
		carried[r], out[i] = c, c
	}
	stats.Time = time.Since(started)
	return out, stats
}

// carry returns r for this head, given the results already carried, and
// parent, a reader of this head's state.
func (p *Preexecutor) carry(r *Result, carried map[*Result]*Result, parent *versionedStateReader) *Result {
	cp := *r
	cp.context = p.context
	if !r.Usable() {
		return &cp
	}
	refuse := func() *Result {
		cp.predictOnly = true
		return &cp
	}
	// the basis: the earlier results of the sender still pending, carried;
	// the others must have been included
	keep := make(map[int]bool, len(r.basis))
	cp.basis = nil
	for i, b := range r.basis {
		if cb := carried[b]; cb != nil && cb.Usable() {
			keep[i] = true
			cp.basis = append(cp.basis, cb)
			continue
		}
		if b.pay == nil {
			return refuse()
		}
		n, err := parent.base(key{addr: b.pay.sender, field: nonce})
		if err != nil || n.uint() <= b.pay.nonce {
			return refuse() // neither pending nor included
		}
	}
	loads, cloned := r.loads, false
	for k, v := range r.reads {
		lk := k
		if _, ok := r.loads[k]; !ok && k.field != storage {
			lk = key{addr: k.addr, field: exists} // an absent account
		}
		l, recorded := r.loads[lk]
		switch {
		case recorded && l.pos == -1:
			continue // a system call's write
		case recorded && keep[l.pos]:
			// a pending basis wrote it; a balance it changed additively
			// was made absolute on the earlier parent state
			if k.field == balance && r.basis[l.pos].deltas[k.addr] != nil {
				return refuse()
			}
			continue
		}
		// read from the parent state, or written by a basis included since
		if now, err := parent.base(k); err != nil || now != v {
			return refuse()
		}
	}
	// what included bases wrote is now the parent's
	for k, l := range r.loads {
		if l.pos >= 0 && !keep[l.pos] {
			if !cloned {
				loads, cloned = maps.Clone(r.loads), true
			}
			loads[k] = load{pos: fromParent, val: l.val}
		}
	}
	cp.loads = loads
	return &cp
}
