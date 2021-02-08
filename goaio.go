package goaio

import (
	"errors"
	"fmt"
	"net"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"
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
	ErrBusy          = errors.New("busy")
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
	w             *aioContextQueue
	r             *aioContextQueue
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

type aioContextQueue struct {
	head  int
	tail  int
	queue []aioContext
}

func newAioContextQueue(max int) *aioContextQueue {
	return &aioContextQueue{
		queue: make([]aioContext, max+1, max+1),
	}
}

func (this *aioContextQueue) empty() bool {
	return this.head == this.tail
}

func (this *aioContextQueue) add(c aioContext) error {

	if (this.tail+1)%len(this.queue) == this.head {
		return ErrBusy
	} else {
		this.queue[this.tail] = c
		this.tail = (this.tail + 1) % len(this.queue)
		return nil
	}
}

func (this *aioContextQueue) front() *aioContext {
	return &this.queue[this.head]
}

func (this *aioContextQueue) popFront() {
	if this.head != this.tail {
		this.queue[this.tail].buff = nil
		this.queue[this.tail].context = nil
		this.head = (this.head + 1) % len(this.queue)
	}
}

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
	return this.readable && !this.r.empty()
}

func (this *AIOConn) canWrite() bool {
	return this.writeable && !this.w.empty()
}

func (this *AIOConn) Send(buff []byte, context interface{}) error {
	this.Lock()
	defer this.Unlock()

	if this.closed {
		return ErrConnClosed
	} else {
		if err := this.w.add(aioContext{
			buff:    buff,
			context: context,
		}); nil != err {
			return err
		}

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

		if err := this.r.add(aioContext{
			buff:    buff,
			context: context,
		}); nil != err {
			return err
		}

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
	c := this.r.front()
	this.Unlock()
	ver := this.readableVer
	size, err := syscall.Read(this.fd, c.buff)
	this.Lock()
	if err == syscall.EINTR {
		return
	} else if size == 0 || (err != nil && err != syscall.EAGAIN) {
		this.r.popFront()
		this.service.postCompleteStatus(this, size, fmt.Errorf("%d", err), c.context)
	} else if err == syscall.EAGAIN {
		if ver == this.readableVer {
			this.readable = false
		}
	} else {
		this.r.popFront()
		this.service.postCompleteStatus(this, size, nil, c.context)
	}
}

func (this *AIOConn) doWrite() {
	c := this.w.front()
	this.Unlock()
	ver := this.writeableVer
	size, err := syscall.Write(this.fd, c.buff)
	this.Lock()
	if err == syscall.EINTR {
		return
	} else if size == 0 || (err != nil && err != syscall.EAGAIN) {
		this.w.popFront()
		this.service.postCompleteStatus(this, size, fmt.Errorf("%d", err), c.context)
	} else if err == syscall.EAGAIN {
		if ver == this.writeableVer {
			this.writeable = false
			this.service.poller.enableWrite(this)
		}
	} else {
		if len(c.buff) == size {
			this.w.popFront()
			this.service.postCompleteStatus(this, size, nil, c.context)
		} else {
			c.buff = c.buff[size:]
			if ver == this.writeableVer {
				this.writeable = false
				this.service.poller.enableWrite(this)
			}
		}
	}

}

func (this *AIOConn) Do() {
	this.Lock()
	defer this.Unlock()

	if this.closed {
		for !this.r.empty() {
			c := this.r.front()
			this.service.postCompleteStatus(this, 0, ErrConnClosed, c.context)
			this.r.popFront()
		}
		for !this.w.empty() {
			c := this.w.front()
			this.service.postCompleteStatus(this, 0, ErrConnClosed, c.context)
			this.w.popFront()
		}
		this.service.unwatch(this)
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
	mu                  sync.Mutex
	cond                *sync.Cond
	completeQueue       aioResultList
	freeList            aioResultList
	tq                  *taskQueue
	poller              pollerI
	closed              int32
	waitCount           int
	conns               map[int]uintptr
	waitgroup           sync.WaitGroup
	closeOnce           sync.Once
	completeQueueClosed bool
}

func NewAIOService(worker int) *AIOService {
	if poller, err := openPoller(); nil == err {
		s := &AIOService{}
		s.cond = sync.NewCond(&s.mu)
		s.tq = NewTaskQueue()
		s.poller = poller
		s.conns = map[int]uintptr{}
		if worker <= 0 {
			worker = 1
		}

		for i := 0; i < worker; i++ {
			go func() {
				s.waitgroup.Add(1)
				for {
					v, err := s.tq.pop()
					if nil != err {
						return
					} else {
						v.Do()
					}
				}
				s.waitgroup.Done()
			}()
		}

		go s.poller.wait(&s.closed)

		return s
	} else {
		return nil
	}
}

func (this *AIOService) unwatch(c *AIOConn) {
	this.mu.Lock()
	defer this.mu.Unlock()
	delete(this.conns, c.fd)
	this.poller.unwatch(c)
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
		r:         newAioContextQueue(100),
		w:         newAioContextQueue(100),
	}

	if this.poller.watch(cc) {
		this.mu.Lock()
		this.conns[fd] = reflect.ValueOf(cc).Pointer()
		runtime.SetFinalizer(cc, func(cc *AIOConn) {
			cc.Close()
		})
		this.mu.Unlock()
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
	if this.completeQueueClosed {
		this.mu.Unlock()
	} else {
		r := this.freeList.pop()
		if nil == r {
			r = &aioResult{}
		}

		r.conn = c
		r.context = context
		r.err = err
		r.bytestransfer = bytestransfer

		this.completeQueue.push(r)

		waitCount := this.waitCount

		this.mu.Unlock()

		if waitCount > 0 {
			this.cond.Signal()
		}
	}
}

func (this *AIOService) GetCompleteStatus() (err error, c *AIOConn, bytestransfer int, context interface{}) {
	this.mu.Lock()

	for this.completeQueue.empty() {
		if this.completeQueueClosed {
			err = ErrWatcherClosed
			this.mu.Unlock()
			return
		}
		this.waitCount++
		this.cond.Wait()
		this.waitCount--
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

func (this *AIOService) Close() {
	this.closeOnce.Do(func() {
		atomic.StoreInt32(&this.closed, 1)
		this.poller.trigger()
		this.mu.Lock()
		/*   关闭所有AIOConn
		 *   最终在AIOConn.Do中向所有待处理的AIO返回ErrConnClosed
		 */

		for _, v := range this.conns {
			(*AIOConn)(unsafe.Pointer(v)).Close()
		}
		this.mu.Unlock()

		this.tq.close()
		//等待worker处理完所有的AIOConn清理
		this.waitgroup.Wait()

		//所有的待处理的AIO在此时已经被投递到completeQueue，可以关闭completeQueue。
		this.mu.Lock()
		this.completeQueueClosed = true
		this.mu.Unlock()
		this.cond.Broadcast()
	})
}
