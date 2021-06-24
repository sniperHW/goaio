package goaio

import (
	"sync"
)

type listItem struct {
	pprev *listItem
	nnext *listItem
	v     interface{}
}

var itemPool *sync.Pool = &sync.Pool{
	New: func() interface{} {
		return &listItem{}
	},
}

func getItem(v interface{}) (i *listItem) {
	i = itemPool.Get().(*listItem)
	i.v = v
	return i
}

func putItem(i *listItem) {
	i.v = nil
	itemPool.Put(i)
}

type linkList struct {
	head *listItem
}

func (l *linkList) push(v interface{}) {
	n := getItem(v)
	tail := l.head.pprev
	n.nnext = tail.nnext
	n.pprev = tail
	tail.nnext = n
	l.head.pprev = n
}

func (l *linkList) pop() interface{} {
	if l.head.nnext == l.head {
		return nil
	} else {
		first := l.head.nnext
		l.removeContext(first)
		v := first.v
		putItem(first)
		return v
	}
}

func (l *linkList) removeContext(n *listItem) {
	if nil != n.nnext && nil != n.pprev && n.nnext != n && n.pprev != n {
		next := n.nnext
		prev := n.pprev
		prev.nnext = next
		next.pprev = prev
		n.nnext = nil
		n.pprev = nil
	}
}

func newList() *linkList {
	l := &linkList{head: &listItem{}}
	l.head.nnext = l.head
	l.head.pprev = l.head
	return l
}

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
	freeRoutines    *linkList
	taskQueue       *linkList
}

func newTaskPool(maxRoutineCount int) *taskPool {
	if maxRoutineCount <= 0 {
		maxRoutineCount = 8
	}
	return &taskPool{
		maxRoutineCount: maxRoutineCount,
		freeRoutines:    newList(),
		taskQueue:       newList(),
	}
}

func (p *taskPool) putRoutine(r *routine) (bool, func()) {
	p.Lock()
	if p.die {
		p.Unlock()
		return false, nil
	} else {
		v := p.taskQueue.pop()
		if nil != v {
			p.Unlock()
			return true, v.(func())
		} else {
			p.freeRoutines.push(r)
			p.Unlock()
		}
		return true, nil
	}
}

func (p *taskPool) getRoutine() *routine {
	if f := p.freeRoutines.pop(); nil != f {
		return f.(*routine)
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
		p.taskQueue.push(task)
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
	for v := p.freeRoutines.pop(); nil != v; v = p.freeRoutines.pop() {
		close(v.(*routine).taskCh)
	}
}
