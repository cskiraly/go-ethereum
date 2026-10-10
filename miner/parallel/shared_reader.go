package parallel

import (
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
)

// SharedReader caches the parent state reads of a build for every reader of
// it: the workers' executions and the block state the committer applies
// results to. Workers usually read an account before the committer applies a
// result touching it, so the committer's loads become cache hits.
type SharedReader struct {
	state.Reader
	accounts sync.Map // common.Address -> *types.StateAccount, nil if absent
	slots    sync.Map // slotKey -> common.Hash
	loading  sync.Map // common.Address or slotKey -> *pending, while it is read
}

type slotKey struct {
	addr common.Address
	slot common.Hash
}

// pending is a read in progress; concurrent readers of the same key wait for it
// instead of reading it again.
type pending struct {
	done chan struct{}
	err  error
}

var _ state.Reader = (*SharedReader)(nil)

// NewSharedReader wraps reader, which must be safe for concurrent use.
func NewSharedReader(reader state.Reader) *SharedReader {
	return &SharedReader{Reader: reader}
}

// once reads the key k with read unless it is being read already, in which
// case it waits for that read. read stores what it read in the cache.
func (r *SharedReader) once(k any, read func() error) error {
	l := &pending{done: make(chan struct{})}
	if v, busy := r.loading.LoadOrStore(k, l); busy {
		l = v.(*pending)
		<-l.done
		return l.err
	}
	l.err = read()
	r.loading.Delete(k)
	close(l.done)
	return l.err
}

// Account returns the parent account at addr, from the cache if loaded.
func (r *SharedReader) Account(addr common.Address) (*types.StateAccount, error) {
	for {
		if v, ok := r.accounts.Load(addr); ok {
			if acct := v.(*types.StateAccount); acct != nil {
				return acct.Copy(), nil
			}
			return nil, nil
		}
		err := r.once(addr, func() error {
			if _, ok := r.accounts.Load(addr); ok {
				return nil // read while this caller started
			}
			acct, err := r.Reader.Account(addr)
			if err != nil {
				return err
			}
			if acct != nil {
				acct = acct.Copy()
			}
			r.accounts.Store(addr, acct)
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
}

// Storage returns the parent value of slot at addr, from the cache if loaded.
func (r *SharedReader) Storage(addr common.Address, slot common.Hash) (common.Hash, error) {
	k := slotKey{addr, slot}
	for {
		if v, ok := r.slots.Load(k); ok {
			return v.(common.Hash), nil
		}
		err := r.once(k, func() error {
			if _, ok := r.slots.Load(k); ok {
				return nil
			}
			val, err := r.Reader.Storage(addr, slot)
			if err != nil {
				return err
			}
			r.slots.Store(k, val)
			return nil
		})
		if err != nil {
			return common.Hash{}, err
		}
	}
}

// Load reads the given accounts and slots into the cache concurrently, with
// up to n reads at a time, and returns when all are cached (or failed: a
// failed read is left to the caller that needs it).
func (r *SharedReader) Load(accounts []common.Address, slots []slotKey, n int) {
	var (
		wg  sync.WaitGroup
		sem = make(chan struct{}, max(1, n))
	)
	run := func(f func()) {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer func() { <-sem; wg.Done() }()
			f()
		}()
	}
	for _, addr := range accounts {
		if _, ok := r.accounts.Load(addr); !ok {
			run(func() { r.Account(addr) })
		}
	}
	for _, k := range slots {
		if _, ok := r.slots.Load(k); !ok {
			run(func() { r.Storage(k.addr, k.slot) })
		}
	}
	wg.Wait()
}
