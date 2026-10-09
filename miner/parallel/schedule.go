package parallel

import (
	"container/heap"
	"sync"
	"sync/atomic"
	"time"
)

// scheduler executes tasks, lowest position first, with at most `workers` of
// them running at a time, and reports each finished task on finished. Every
// execution gets its own goroutine and holds one of the tokens while it runs;
// an execution that waits for another one gives its token back meanwhile.
type scheduler struct {
	store     *store
	run       func(*Task) *Result
	finished  chan *Task
	inlineGas uint64 // cheap tasks are left to the committer

	tokens chan struct{}
	quit   chan struct{} // closed by stop: waiting executions give up

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
		tokens:    make(chan struct{}, workers),
		quit:      make(chan struct{}),
	}
	for range workers {
		sched.tokens <- struct{}{}
	}
	sched.cond = sync.NewCond(&sched.mu)
	sched.wg.Add(1)
	go sched.dispatch()
	return sched
}

// dispatch starts the ready tasks in position order as tokens become free.
func (s *scheduler) dispatch() {
	defer s.wg.Done()
	for {
		<-s.tokens
		t := s.getTaskReadyForExecution()
		if t == nil {
			s.tokens <- struct{}{}
			return
		}
		s.wg.Add(1)
		go s.execute(t)
	}
}

func (s *scheduler) execute(t *Task) {
	defer s.wg.Done()
	started := time.Now()
	res := s.run(t)
	s.executions.Add(1)
	s.executionTime.Add(int64(time.Since(started)))
	s.finish(t, res)
	s.tokens <- struct{}{}
}

// suspend gives the calling execution's token back until done is closed,
// then takes one again. It reports false if the scheduler stopped meanwhile.
func (s *scheduler) suspend(done <-chan struct{}) bool {
	started := time.Now()
	s.waits.Add(1)
	s.tokens <- struct{}{}
	defer func() { s.waitTime.Add(int64(time.Since(started))) }()
	select {
	case <-done:
	case <-s.quit:
		<-s.tokens
		return false
	}
	select {
	case <-s.tokens:
		return true
	case <-s.quit:
		<-s.tokens
		return false
	}
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
	s.cond.Signal()
}

// getTaskReadyForExecution returns the lowest released task, or nil once the
// scheduler stopped.
func (s *scheduler) getTaskReadyForExecution() *Task {
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		for len(s.ready) == 0 && !s.closed {
			s.cond.Wait()
		}
		if s.closed {
			return nil
		}
		task := heap.Pop(&s.ready).(*Task)
		if task.released {
			return task
		}
	}
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
