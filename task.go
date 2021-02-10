package goaio

import (
	"errors"
	"sync"
)

var (
	Error_TaskQueue_Closed = errors.New("task queue closed")
)

type taskQueue struct {
	mu        sync.Mutex
	cond      *sync.Cond
	head      *AIOConn
	closed    bool
	closeOnce sync.Once
	waitCount int
}

func NewTaskQueue() *taskQueue {
	q := &taskQueue{}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (this *taskQueue) close() {
	this.closeOnce.Do(func() {
		this.mu.Lock()
		this.closed = true
		this.mu.Unlock()
		this.cond.Broadcast()
	})
}

func (this *taskQueue) push(t *AIOConn) error {
	this.mu.Lock()
	if this.closed {
		this.mu.Unlock()
		return Error_TaskQueue_Closed
	}

	var head *AIOConn
	if this.head == nil {
		head = t
	} else {
		head = this.head.nnext
		this.head.nnext = t
	}
	t.nnext = head
	this.head = t

	waitCount := this.waitCount
	this.mu.Unlock()

	if waitCount > 0 {
		this.cond.Signal()
	}

	return nil
}

func (this *taskQueue) pop() (*AIOConn, error) {
	this.mu.Lock()
	for this.head == nil {
		if this.closed {
			this.mu.Unlock()
			return nil, Error_TaskQueue_Closed
		} else {
			this.waitCount++
			this.cond.Wait()
			this.waitCount--
		}
	}

	e := this.head.nnext
	if e == this.head {
		this.head = nil
	} else {
		this.head.nnext = e.nnext
	}

	e.nnext = nil

	this.mu.Unlock()

	return e, nil
}
