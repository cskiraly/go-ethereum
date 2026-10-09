package parallel

import (
	"container/heap"
	"sync"
	"sync/atomic"
	"time"
)

// scheduler executes tasks, lowest position first, with at most `workers` of
// them running at a time, and reports each finished task on finished. Every
// execution gets its own goroutine and holds one of the slots while it runs;
// an execution that waits for another one gives its slot back meanwhile, and
// gets the next free one before any new execution does.
type scheduler struct {
	store     *store
	run       func(*Task) *Result
	finished  chan *Task
	inlineGas uint64 // cheap tasks are left to the committer

	quit     chan struct{} // closed by stop: waiting executions give up
	free     int           // free slots, guarded by mu
	resuming int           // executions waiting for a slot to continue, guarded by mu

	mu            sync.Mutex
	cond          *sync.Cond
	ready         taskHeap
	closed        bool
	executions    atomic.Int64
	executionTime atomic.Int64
	waits         atomic.Int64
	waitTime      atomic.Int64
	wg            sync.WaitGroup
}

func newScheduler(s *store, workers int, run func(*Task) *Result, inlineGas uint64) *scheduler {
	sched := &scheduler{
		store:     s,
		run:       run,
		inlineGas: inlineGas,
		quit:      make(chan struct{}),
		free:      workers,
	}
	sched.cond = sync.NewCond(&sched.mu)
	sched.wg.Add(1)
	go sched.dispatch()
	return sched
}

// dispatch starts the lowest ready task whenever a slot is free and no
// waiting execution is about to take it. The slot and the task are taken
// together, so a task released meanwhile is not overtaken.
func (s *scheduler) dispatch() {
	defer s.wg.Done()
	for {
		s.mu.Lock()
		for !s.closed && (s.free <= s.resuming || !s.hasReady()) {
			s.cond.Wait()
		}
		if s.closed {
			s.mu.Unlock()
			return
		}
		t := heap.Pop(&s.ready).(*Task)
		s.free--
		s.mu.Unlock()
		s.wg.Add(1)
		go s.execute(t)
	}
}

// hasReady drops tasks that were never released and reports whether one is
// left. The caller holds mu.
func (s *scheduler) hasReady() bool {
	for len(s.ready) > 0 && !s.ready[0].isReleased() {
		heap.Pop(&s.ready)
	}
	return len(s.ready) > 0
}

func (s *scheduler) release1() {
	s.mu.Lock()
	s.free++
	s.mu.Unlock()
	s.cond.Broadcast()
}

func (s *scheduler) execute(t *Task) {
	defer s.wg.Done()
	started := time.Now()
	res := s.run(t)
	s.executions.Add(1)
	s.executionTime.Add(int64(time.Since(started)))
	s.finish(t, res)
	s.release1()
}

// suspend gives the calling execution's slot back until done is closed, then
// takes the next free one. It reports false if the scheduler stopped
// meanwhile.
func (s *scheduler) suspend(done <-chan struct{}) bool {
	started := time.Now()
	s.waits.Add(1)
	defer func() { s.waitTime.Add(int64(time.Since(started))) }()
	s.release1()
	select {
	case <-done:
	case <-s.quit:
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resuming++
	for s.free == 0 && !s.closed {
		s.cond.Wait()
	}
	s.resuming--
	if s.closed {
		return false
	}
	s.free--
	s.cond.Broadcast()
	return true
}

// start releases every task that has no predecessor to wait for. Cheap tasks
// are never released, and do not hold back their successors.
func (s *scheduler) start(tasks []*Task) {
	// every task finishes once, plus once per requeue
	s.finished = make(chan *Task, len(tasks)*(1+maxRequeues))
	for _, t := range tasks {
		if !t.cheap(s.inlineGas) && (t.prev == nil || t.prev.cheap(s.inlineGas)) {
			s.release(t)
		}
	}
}

// release marks t as released and pushes it to the
// ready queue if it hasnt been released before.
func (s *scheduler) release(t *Task) {
	t.mu.Lock()
	released := t.released
	t.released = true
	t.mu.Unlock()
	if !released {
		s.push(t)
	}
}

// stop lets running executions finish and stops the workers.
func (s *scheduler) stop() (int, time.Duration) {
	s.mu.Lock()
	s.closed = true
	s.ready = nil
	s.mu.Unlock()
	s.cond.Broadcast()
	close(s.quit)
	s.wg.Wait()
	return int(s.executions.Load()), time.Duration(s.executionTime.Load())
}

func (s *scheduler) push(t *Task) {
	s.mu.Lock()
	heap.Push(&s.ready, t)
	s.mu.Unlock()
	s.cond.Broadcast()
}

// finish publishes the result of t, reports it and releases the same-sender
// successor of t.
func (s *scheduler) finish(t *Task, res *Result) {
	t.mu.Lock()
	s.store.publish(t.Position, res.writes, res.codes)
	t.result = res
	t.mu.Unlock()
	t.markDone()
	s.finished <- t
	if t.next != nil && !t.next.cheap(s.inlineGas) {
		s.release(t.next)
	}
}

type taskHeap []*Task

func (h taskHeap) Len() int {
	return len(h)
}
func (h taskHeap) Less(i, j int) bool {
	return h[i].Position < h[j].Position
}
func (h taskHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}
func (h *taskHeap) Push(x any) {
	*h = append(*h, x.(*Task))
}

func (h *taskHeap) Pop() any {
	old := *h
	t := old[len(old)-1]
	*h = old[:len(old)-1]
	return t
}
