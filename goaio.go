package goaio

import (
	"container/list"
	"errors"
	"fmt"
	"net"
	"runtime"
	"sync"
	"syscall"
)

var (
	ErrEof           = errors.New("EOF")
	ErrRecvTimeout   = errors.New("RecvTimeout")
	ErrSendTimeout   = errors.New("SendTimetout")
	ErrTimeout       = errors.New("Timeout")
	ErrConnClosed    = errors.New("conn closed")
	ErrWatcherClosed = errors.New("watcher closed")
	ErrUnsupportConn = errors.New("net.Conn does implement net.RawConn")
	ErrIoPending     = errors.New("io pending")
	ErrSendBuffNil   = errors.New("send buff is nil")
	ErrWatchFailed   = errors.New("watch failed")
)

const (
	EV_READ  = int(1 << 1)
	EV_WRITE = int(1 << 2)
	EV_ERROR = int(1 << 3)
)

type AIOConn struct {
	sync.Mutex
	fd            int
	rawconn       net.Conn
	readable      bool
	readableVer   int
	writeable     bool
	writeableVer  int
	w             *list.List
	r             *list.List
	service       *AIOService
	doing         bool
	closed        bool
	pollerVersion int32
	closeOnce     sync.Once
}

type aioContext struct {
	buff    []byte
	context interface{}
}

/*
type aioContextQueue struct {
	head  int
	tail  int
	queue []aioContext
}

func (this *aioContextQueue) add(c aioContext) error {

}

func (this *aioContextQueue) front() {

}

func (this *aioContextQueue) popFront() {

}*/

type aioResult struct {
	ppnext        *aioResult
	conn          *AIOConn
	context       interface{}
	err           error
	bytestransfer int
}

type aioResultList struct {
	head *aioResult
}

func (this *aioResultList) empty() bool {
	return this.head == nil
}

func (this *aioResultList) push(item *aioResult) {
	var head *aioResult
	if this.head == nil {
		head = item
	} else {
		head = this.head.ppnext
		this.head.ppnext = item
	}
	item.ppnext = head
	this.head = item
}

func (this *aioResultList) pop() *aioResult {
	if this.head == nil {
		return nil
	} else {
		item := this.head.ppnext
		if item == this.head {
			this.head = nil
		} else {
			this.head.ppnext = item.ppnext
		}

		item.ppnext = nil
		return item
	}
}

func (this *AIOConn) Close() {
	this.closeOnce.Do(func() {
		this.Lock()
		defer this.Unlock()
		this.closed = true
		runtime.SetFinalizer(this, nil)
		if !this.doing {
			this.doing = true
			this.service.pushIOTask(this)
		}
	})
}

func (this *AIOConn) canRead() bool {
	return this.readable && this.r.Len() > 0
}

func (this *AIOConn) canWrite() bool {
	return this.writeable && this.w.Len() > 0
}

func (this *AIOConn) Send(buff []byte, context interface{}) error {
	this.Lock()
	defer this.Unlock()

	if this.closed {
		return ErrConnClosed
	} else {

		this.w.PushBack(aioContext{
			buff:    buff,
			context: context,
		})

		if this.writeable && !this.doing {
			this.doing = true
			this.service.pushIOTask(this)
		}

		return nil
	}
}

func (this *AIOConn) Recv(buff []byte, context interface{}) error {
	this.Lock()
	defer this.Unlock()

	if this.closed {
		return ErrConnClosed
	} else {

		this.r.PushBack(aioContext{
			buff:    buff,
			context: context,
		})

		if this.readable && !this.doing {
			this.doing = true
			this.service.pushIOTask(this)
		}

		return nil
	}
}

func (this *AIOConn) onActive(ev int) {

	this.Lock()
	defer this.Unlock()

	if ev&EV_READ != 0 || ev&EV_ERROR != 0 {
		this.readable = true
		this.readableVer++
	}

	if ev&EV_WRITE != 0 || ev&EV_ERROR != 0 {
		this.writeable = true
		this.writeableVer++
		this.service.poller.disableWrite(this)
	}

	if !this.doing {
		this.doing = true
		this.service.pushIOTask(this)
	}

}

func (this *AIOConn) doRead() {
	c := this.r.Front().Value.(aioContext)
	this.Unlock()
	ver := this.readableVer
	size, err := syscall.Read(this.fd, c.buff)
	this.Lock()
	if err == syscall.EINTR {
		return
	} else if size == 0 || (err != nil && err != syscall.EAGAIN) {
		this.r.Remove(this.r.Front())
		this.service.postCompleteStatus(this, size, fmt.Errorf("%d", err), c.context)
	} else if err == syscall.EAGAIN {
		if ver == this.readableVer {
			this.readable = false
		}
	} else {
		this.r.Remove(this.r.Front())
		this.service.postCompleteStatus(this, size, nil, c.context)
	}
}

func (this *AIOConn) doWrite() {
	c := this.w.Front().Value.(aioContext)
	this.Unlock()
	ver := this.writeableVer
	size, err := syscall.Write(this.fd, c.buff)
	this.Lock()
	if err == syscall.EINTR {
		return
	} else if size == 0 || (err != nil && err != syscall.EAGAIN) {
		this.w.Remove(this.w.Front())
		this.service.postCompleteStatus(this, size, fmt.Errorf("%d", err), c.context)
	} else if err == syscall.EAGAIN {
		if ver == this.writeableVer {
			this.writeable = false
			this.service.poller.enableWrite(this)
		}
	} else {
		this.w.Remove(this.w.Front())
		this.service.postCompleteStatus(this, size, nil, c.context)
	}

}

func (this *AIOConn) Do() {
	this.Lock()
	defer this.Unlock()

	if this.closed {
		for v := this.r.Front(); nil != v; v = this.r.Front() {
			c := v.Value.(aioContext)
			this.service.postCompleteStatus(this, 0, ErrConnClosed, c.context)
			this.r.Remove(v)
		}

		for v := this.w.Front(); nil != v; v = this.w.Front() {
			c := v.Value.(aioContext)
			this.service.postCompleteStatus(this, 0, ErrConnClosed, c.context)
			this.w.Remove(v)
		}

		this.service.poller.unwatch(this)
		this.rawconn.Close()

	} else {
		if this.canRead() {
			this.doRead()
		}

		if this.canWrite() {
			this.doWrite()
		}

		if this.closed || this.canRead() || this.canWrite() {
			this.service.pushIOTask(this)
		} else {
			this.doing = false
		}
	}
}

type AIOService struct {
	mu            sync.Mutex
	cond          *sync.Cond
	completeQueue aioResultList
	freeList      aioResultList
	tq            *taskQueue
	poller        pollerI
	closed        int32
}

func NewAIOService(worker int) *AIOService {
	if poller, err := openPoller(); nil == err {
		s := &AIOService{}
		s.cond = sync.NewCond(&s.mu)
		s.tq = NewTaskQueue()
		s.poller = poller
		if worker <= 0 {
			worker = 1
		}

		for i := 0; i < worker; i++ {
			go func() {
				for {
					v, err := s.tq.pop()
					if nil != err {
						return
					} else {
						v.Do()
					}
				}
			}()
		}

		go s.poller.wait(&s.closed)

		return s
	} else {
		return nil
	}
}

func (this *AIOService) Bind(conn net.Conn) *AIOConn {
	c, ok := conn.(interface {
		SyscallConn() (syscall.RawConn, error)
	})

	if !ok {
		return nil
	}

	rawconn, err := c.SyscallConn()
	if err != nil {
		return nil
	}

	var fd int

	if err := rawconn.Control(func(s uintptr) {
		fd = int(s)
	}); err != nil {
		return nil
	}

	syscall.SetNonblock(fd, true)

	cc := &AIOConn{
		fd:        fd,
		readable:  false,
		writeable: true,
		rawconn:   conn,
		service:   this,
		r:         list.New(),
		w:         list.New(),
	}

	if this.poller.watch(cc) {
		runtime.SetFinalizer(cc, func(cc *AIOConn) {
			cc.Close()
		})
		return cc
	} else {
		return nil
	}
}

func (this *AIOService) pushIOTask(c *AIOConn) {
	this.tq.push(c)
}

func (this *AIOService) postCompleteStatus(c *AIOConn, bytestransfer int, err error, context interface{}) {
	this.mu.Lock()

	r := this.freeList.pop()
	if nil == r {
		r = &aioResult{}
	}

	r.conn = c
	r.context = context
	r.err = err
	r.bytestransfer = bytestransfer

	this.completeQueue.push(r)

	this.mu.Unlock()
	this.cond.Signal()
}

func (this *AIOService) GetCompleteStatus() (err error, c *AIOConn, bytestransfer int, context interface{}) {
	this.mu.Lock()

	for this.completeQueue.empty() {
		this.cond.Wait()
	}

	e := this.completeQueue.pop()

	err = e.err
	c = e.conn
	bytestransfer = e.bytestransfer
	context = e.context

	this.freeList.push(e)

	this.mu.Unlock()

	return
}
