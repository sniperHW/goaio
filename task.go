package goaio

import (
	"container/list"
	"errors"
	"sync"
)

var (
	Error_TaskQueue_Closed = errors.New("task queue closed")
)

type TaskI interface {
	Do()
}

type taskQueue struct {
	mu        sync.Mutex
	cond      *sync.Cond
	l         *list.List
	closed    bool
	closeOnce sync.Once
	waitCount int
}

func NewTaskQueue() *taskQueue {
	q := &taskQueue{}
	q.cond = sync.NewCond(&q.mu)
	q.l = list.New()
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

func (this *taskQueue) push(t TaskI) error {
	this.mu.Lock()
	if this.closed {
		this.mu.Unlock()
		return Error_TaskQueue_Closed
	}
	this.l.PushBack(t)
	waitCount := this.waitCount
	this.mu.Unlock()

	if waitCount > 0 {
		this.cond.Signal()
	}

	return nil
}

func (this *taskQueue) pop() (TaskI, error) {
	this.mu.Lock()
	for this.l.Len() == 0 {
		if this.closed {
			this.mu.Unlock()
			return nil, Error_TaskQueue_Closed
		} else {
			this.waitCount++
			this.cond.Wait()
			this.waitCount--
		}
	}

	e := this.l.Front()
	this.l.Remove(e)

	this.mu.Unlock()
	return e.Value.(TaskI), nil
}
