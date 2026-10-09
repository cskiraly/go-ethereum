package parallel

import (
	"slices"

	"github.com/ethereum/go-ethereum/common"
)

// writeWaiter makes reads wait for the lower positions predicted to write the
// key read, so that they read the value those will write instead of one
// they are about to replace. Waits only ever point to lower positions, so they
// cannot form a cycle.
type writeWaiter struct {
	writers map[key][]*Task            // predicted exact writers of each key, by position
	deltas  map[common.Address][]*Task // predicted delta writers of each balance, by position
	sched   *scheduler
}

func newWriteWaiter(tasks []*Task, sched *scheduler) *writeWaiter {
	w := &writeWaiter{writers: make(map[key][]*Task), deltas: make(map[common.Address][]*Task), sched: sched}
	for _, t := range tasks {
		for _, k := range t.predicted {
			w.writers[k] = append(w.writers[k], t)
		}
		for _, a := range t.deltas {
			w.deltas[a] = append(w.deltas[a], t)
		}
	}
	byPosition := func(a, b *Task) int { return a.Position - b.Position }
	for _, ts := range w.writers {
		slices.SortFunc(ts, byPosition)
	}
	for _, ts := range w.deltas {
		slices.SortFunc(ts, byPosition)
	}
	return w
}

// wait returns once no lower position than pos predicted to write k is
// still pending, or the run stopped.
func (w *writeWaiter) wait(k key, pos int) {
	w.waitFor(w.writers[k], pos)
}

// waitDeltas is wait for the predicted delta writers of a balance.
func (w *writeWaiter) waitDeltas(addr common.Address, pos int) {
	w.waitFor(w.deltas[addr], pos)
}

func (w *writeWaiter) waitFor(ts []*Task, pos int) {
	for {
		var pending *Task
		for i := len(ts) - 1; i >= 0; i-- {
			if ts[i].Position < pos && !ts[i].isDone() {
				pending = ts[i]
				break
			}
		}
		if pending == nil || !w.sched.suspend(pending.done) {
			return
		}
	}
}
