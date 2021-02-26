package goaio

import (
	"errors"
	"sync"
)

var (
	Error_TaskQueue_Closed = errors.New("task queue closed")
)

type TaskQueue struct {
	mu        sync.Mutex
	cond      *sync.Cond
	closed    bool
	closeOnce sync.Once
	waitCount int
	queue     []interface{}
}

func NewTaskQueue() *TaskQueue {
	q := &TaskQueue{}
	q.cond = sync.NewCond(&q.mu)
	q.queue = make([]interface{}, 0, 512)
	return q
}

func (this *TaskQueue) Close() {
	this.closeOnce.Do(func() {
		this.mu.Lock()
		this.closed = true
		this.mu.Unlock()
		this.cond.Broadcast()
	})
}

func (this *TaskQueue) Push(t interface{}) error {
	this.mu.Lock()
	if this.closed {
		this.mu.Unlock()
		return Error_TaskQueue_Closed
	}

	this.queue = append(this.queue, t)

	waitCount := this.waitCount
	this.mu.Unlock()

	if waitCount > 0 {
		this.cond.Signal()
	}

	return nil
}

func (this *TaskQueue) Pop(queue []interface{}) (out []interface{}, err error) {
	this.mu.Lock()
	for len(this.queue) == 0 {
		if this.closed {
			this.mu.Unlock()
			err = Error_TaskQueue_Closed
			return
		} else {
			this.waitCount++
			this.cond.Wait()
			this.waitCount--
		}
	}

	out = this.queue
	this.queue = queue[0:0]

	this.mu.Unlock()
	return
}
