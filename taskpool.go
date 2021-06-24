package goaio

import (
	"container/list"
	"sync"
)

type routine struct {
	taskCh chan func()
}

func (r *routine) run(p *taskPool) {
	var ok bool
	for task := range r.taskCh {
		task()
		for {
			ok, task = p.putRoutine(r)
			if !ok {
				return
			} else if nil != task {
				task()
			} else {
				break
			}
		}
	}
}

type taskPool struct {
	sync.Mutex
	die             bool
	routineCount    int
	maxRoutineCount int
	freeRoutines    *list.List
	taskQueue       *list.List
}

func newTaskPool(maxRoutineCount int) *taskPool {
	if maxRoutineCount <= 0 {
		maxRoutineCount = 8
	}
	return &taskPool{
		maxRoutineCount: maxRoutineCount,
		freeRoutines:    list.New(),
		taskQueue:       list.New(),
	}
}

func (p *taskPool) putRoutine(r *routine) (bool, func()) {
	p.Lock()
	if p.die {
		p.Unlock()
		return false, nil
	} else {
		f := p.taskQueue.Front()
		if nil != f {
			v := p.taskQueue.Remove(f).(func())
			p.Unlock()
			return true, v
		} else {
			p.freeRoutines.PushBack(r)
			p.Unlock()
		}
		return true, nil
	}
}

func (p *taskPool) getRoutine() *routine {
	if f := p.freeRoutines.Front(); nil != f {
		return p.freeRoutines.Remove(f).(*routine)
	} else {
		return nil
	}
}

func (p *taskPool) addTask(task func()) bool {
	p.Lock()
	if p.die {
		p.Unlock()
		return false
	} else if r := p.getRoutine(); nil != r {
		p.Unlock()
		r.taskCh <- task
		return true
	} else if p.routineCount >= p.maxRoutineCount {
		p.taskQueue.PushBack(task)
		p.Unlock()
		return true
	} else {
		p.routineCount++
		r := &routine{taskCh: make(chan func())}
		p.Unlock()
		go r.run(p)
		r.taskCh <- task
		return true
	}
}

func (p *taskPool) close() {
	p.Lock()
	defer p.Unlock()
	if !p.die {
		p.die = true
	}
	for f := p.freeRoutines.Front(); nil != f; f = p.freeRoutines.Front() {
		v := p.freeRoutines.Remove(f).(*routine)
		close(v.taskCh)
	}
}
