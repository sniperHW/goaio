package goaio

import (
	"errors"
	//"fmt"
	"net"
	//"reflect"
	//"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

var (
	ErrEof           = errors.New("EOF")
	ErrRecvTimeout   = errors.New("RecvTimeout")
	ErrSendTimeout   = errors.New("SendTimetout")
	ErrConnClosed    = errors.New("conn closed")
	ErrServiceClosed = errors.New("service closed")
	ErrUnsupportConn = errors.New("net.Conn does implement net.RawConn")
	ErrBusy          = errors.New("busy")
	ErrWatchFailed   = errors.New("watch failed")
	ErrActiveClose   = errors.New("active close")
	ErrCloseByGC     = errors.New("close by gc")
)

const (
	EV_READ  = int(1 << 1)
	EV_WRITE = int(1 << 2)
	EV_ERROR = int(1 << 3)
)

var (
	DefaultWorkerCount = 4
)

type AIOConn struct {
	sync.Mutex
	nnext         TaskI
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
	sendTimeout   time.Duration
	recvTimeout   time.Duration
	timer         *Timer
	key           interface{}
	reason        error
}

type aioContext struct {
	buff     []byte
	offset   int
	context  interface{}
	deadline time.Time
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

func (this *aioContextQueue) setDeadline(deadline time.Time) bool {
	if this.empty() {
		return false
	} else {
		i := this.head
		for i != this.tail {
			this.queue[i].deadline = deadline
			i = (i + 1) % len(this.queue)
		}
		return true
	}
}

type aioResult struct {
	ppnext        *aioResult
	conn          *AIOConn
	buff          []byte
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

type completetionQueue struct {
	mu            sync.Mutex
	cond          *sync.Cond
	completeQueue aioResultList
	freeList      aioResultList
	closed        bool
	waitCount     int
}

func newCompletetionQueue() *completetionQueue {
	q := &completetionQueue{}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (this *completetionQueue) close() {
	this.mu.Lock()
	this.closed = true
	this.mu.Unlock()
	this.cond.Broadcast()
}

func (this *completetionQueue) postCompleteStatus(c *AIOConn, buff []byte, bytestransfer int, err error, context interface{}) {
	this.mu.Lock()
	if this.closed {
		this.mu.Unlock()
	} else {

		r := this.freeList.pop()
		if nil == r {
			r = &aioResult{}
		}

		r.conn = c
		r.context = context
		r.err = err
		r.buff = buff
		r.bytestransfer = bytestransfer

		this.completeQueue.push(r)

		waitCount := this.waitCount

		this.mu.Unlock()

		if waitCount > 0 {
			this.cond.Signal()
		}
	}
}

func (this *completetionQueue) getCompleteStatus() (c *AIOConn, buff []byte, bytestransfer int, context interface{}, err error) {
	this.mu.Lock()

	for this.completeQueue.empty() {
		if this.closed {
			err = ErrServiceClosed
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
	buff = e.buff
	bytestransfer = e.bytestransfer
	context = e.context

	e.buff = nil
	e.context = nil
	e.conn = nil

	this.freeList.push(e)

	this.mu.Unlock()

	return
}

func (this *AIOConn) GetNext() TaskI {
	return this.nnext
}

func (this *AIOConn) SetNext(next TaskI) {
	this.nnext = next
}

func (this *AIOConn) Close(reason error) {
	this.closeOnce.Do(func() {
		//fmt.Println("close AIOConn", this.fd, this.key, reason)
		this.Lock()
		defer this.Unlock()
		this.closed = true
		this.reason = reason

		if nil != this.timer {
			this.timer.Cancel()
			this.timer = nil
		}

		//runtime.SetFinalizer(this, nil)
		if !this.doing {
			this.doing = true
			this.service.pushIOTask(this)
		}
	})
}

func (this *AIOConn) onTimeout(t *Timer) {
	this.Lock()
	defer this.Unlock()
	now := time.Now()
	if this.timer == t {

		for !this.r.empty() {
			f := this.r.front()
			if now.After(f.deadline) {
				this.r.popFront()
				this.service.postCompleteStatus(this, f.buff, 0, ErrRecvTimeout, f.context)
			} else {
				break
			}
		}

		for !this.w.empty() {
			f := this.w.front()
			if now.After(f.deadline) {
				this.w.popFront()
				this.service.postCompleteStatus(this, f.buff, f.offset, ErrSendTimeout, f.context)
			} else {
				break
			}
		}

		var deadline time.Time
		if !this.r.empty() && this.r.front().deadline.After(deadline) {
			deadline = this.r.front().deadline
		}

		if !this.w.empty() && this.w.front().deadline.After(deadline) {
			deadline = this.w.front().deadline
		}

		if !deadline.IsZero() {
			this.timer = newTimer(now.Sub(deadline), this.onTimeout)
		}

	}
}

func (this *AIOConn) SetRecvTimeout(timeout time.Duration) {
	this.Lock()
	defer this.Unlock()
	this.recvTimeout = timeout
	if timeout != 0 {
		deadline := time.Now().Add(timeout)
		if this.r.setDeadline(deadline) {
			this.timer = newTimer(timeout, this.onTimeout)
		}
	} else {
		this.r.setDeadline(time.Time{})
		if nil != this.timer {
			this.timer.Cancel()
			this.timer = nil
		}
	}
}

func (this *AIOConn) SetSendTimeout(timeout time.Duration) {
	this.Lock()
	defer this.Unlock()
	this.sendTimeout = timeout
	if timeout != 0 {
		deadline := time.Now().Add(timeout)
		if this.r.setDeadline(deadline) {
			this.timer = newTimer(timeout, this.onTimeout)
		}
	} else {
		this.r.setDeadline(time.Time{})
		if nil != this.timer {
			this.timer.Cancel()
			this.timer = nil
		}
	}
}

func (this *AIOConn) getRecvTimeout() time.Duration {
	return this.recvTimeout
}

func (this *AIOConn) getSendTimeout() time.Duration {
	return this.sendTimeout
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

		var deadline time.Time

		timeout := this.getSendTimeout()

		if 0 != timeout {
			deadline = time.Now().Add(timeout)
			if nil == this.timer {
				this.timer = newTimer(timeout, this.onTimeout)
			}
		}

		if err := this.w.add(aioContext{
			buff:     buff,
			context:  context,
			deadline: deadline,
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

		var deadline time.Time

		timeout := this.getRecvTimeout()

		if 0 != timeout {
			deadline = time.Now().Add(timeout)
			if nil == this.timer {
				this.timer = newTimer(timeout, this.onTimeout)
			}
		}

		if err := this.r.add(aioContext{
			buff:     buff,
			context:  context,
			deadline: deadline,
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
	}

	if ev&EV_WRITE != 0 || ev&EV_ERROR != 0 {
		this.writeable = true
		this.service.poller.disableWrite(this)
	}

	if (this.canRead() || this.canWrite()) && !this.doing {
		this.doing = true
		this.service.pushIOTask(this)
	}

}

func (this *AIOConn) doRead() {
	c := this.r.front()
	size, err := syscall.Read(this.fd, c.buff)
	if err == syscall.EINTR {
		return
	} else if size == 0 || (err != nil && err != syscall.EAGAIN) {
		this.r.popFront()
		if size == 0 {
			err = ErrEof
		}
		this.service.postCompleteStatus(this, c.buff, size, err, c.context)
	} else if err == syscall.EAGAIN {
		this.readable = false
	} else {
		this.r.popFront()
		this.service.postCompleteStatus(this, c.buff, size, nil, c.context)
	}
}

func (this *AIOConn) doWrite() {
	c := this.w.front()
	size, err := syscall.Write(this.fd, c.buff[c.offset:])
	if err == syscall.EINTR {
		return
	} else if size == 0 || (err != nil && err != syscall.EAGAIN) {
		this.w.popFront()
		if size == 0 {
			err = ErrEof
		}
		this.service.postCompleteStatus(this, c.buff, c.offset, err, c.context)
	} else if err == syscall.EAGAIN {
		this.writeable = false
		this.service.poller.enableWrite(this)
	} else {
		if len(c.buff[c.offset:]) == size {
			this.w.popFront()
			this.service.postCompleteStatus(this, c.buff, len(c.buff), nil, c.context)
		} else {
			c.offset += size
			this.writeable = false
			this.service.poller.enableWrite(this)
		}
	}

}

func (this *AIOConn) Do() {
	for {
		this.Lock()
		if this.closed {
			for !this.r.empty() {
				c := this.r.front()
				this.service.postCompleteStatus(this, c.buff, 0, this.reason, c.context)
				this.r.popFront()
			}
			for !this.w.empty() {
				c := this.w.front()
				this.service.postCompleteStatus(this, c.buff, c.offset, this.reason, c.context)
				this.w.popFront()
			}
			this.service.unwatch(this)
			this.rawconn.Close()
			this.doing = false
			this.Unlock()
			//fmt.Println("clear AIOConn", this.fd, this.key)
			return
		} else {

			if this.canRead() {
				this.doRead()
			}

			if this.canWrite() {
				this.doWrite()
			}

			if this.canRead() || this.canWrite() {
				this.Unlock()
			} else {
				this.doing = false
				this.Unlock()
				return
			}
		}
	}
}

type AIOService struct {
	sync.Mutex
	completeQueue *completetionQueue
	tq            *taskQueue
	poller        pollerI
	closed        int32
	conns         map[int]*AIOConn
	waitgroup     sync.WaitGroup
	closeOnce     sync.Once
}

func NewAIOService(worker int) *AIOService {
	if poller, err := openPoller(); nil == err {
		s := &AIOService{}
		s.completeQueue = newCompletetionQueue()
		s.tq = NewTaskQueue()
		s.poller = poller
		s.conns = map[int]*AIOConn{}
		if worker <= 0 {
			worker = 1
		}

		for i := 0; i < worker; i++ {
			go func() {
				s.waitgroup.Add(1)
				for {
					v, err := s.tq.pop()
					if nil != err {
						break
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
	this.Lock()
	defer this.Unlock()
	delete(this.conns, c.fd)
	this.poller.unwatch(c)
}

func (this *AIOService) Bind(conn net.Conn, key interface{}) (*AIOConn, error) {
	this.Lock()
	defer this.Unlock()

	if 1 == this.closed {
		return nil, ErrServiceClosed
	}

	c, ok := conn.(interface {
		SyscallConn() (syscall.RawConn, error)
	})

	if !ok {
		return nil, ErrUnsupportConn
	}

	rawconn, err := c.SyscallConn()
	if err != nil {
		return nil, err
	}

	var fd int

	if err := rawconn.Control(func(s uintptr) {
		fd = int(s)
	}); err != nil {
		return nil, err
	}

	syscall.SetNonblock(fd, true)

	cc := &AIOConn{
		fd:        fd,
		readable:  true,
		writeable: true,
		rawconn:   conn,
		service:   this,
		r:         newAioContextQueue(64),
		w:         newAioContextQueue(64),
		key:       key,
	}

	if this.poller.watch(cc) {
		this.conns[fd] = cc
		/*runtime.SetFinalizer(cc, func(c *AIOConn) {
			fmt.Println("GC")
			c.Close(ErrCloseByGC)
		})*/
		return cc, nil
	} else {
		return nil, ErrWatchFailed
	}
}

func (this *AIOService) pushIOTask(c *AIOConn) {
	this.tq.push(c)
}

func (this *AIOService) postCompleteStatus(c *AIOConn, buff []byte, bytestransfer int, err error, context interface{}) {
	this.completeQueue.postCompleteStatus(c, buff, bytestransfer, err, context)
}

func (this *AIOService) GetCompleteStatus() (*AIOConn, []byte, int, interface{}, error) {
	return this.completeQueue.getCompleteStatus()
}

func (this *AIOService) Close() {
	this.closeOnce.Do(func() {

		//fmt.Println("close AIOService")

		this.Lock()
		atomic.StoreInt32(&this.closed, 1)
		this.poller.trigger()
		/*   关闭所有AIOConn
		 *   最终在AIOConn.Do中向所有待处理的AIO返回ErrConnClosed
		 */
		conns := this.conns
		this.conns = nil
		this.Unlock()

		for _, v := range conns {
			(*AIOConn)(unsafe.Pointer(v)).Close(ErrServiceClosed)
		}

		this.tq.close()

		//等待worker处理完所有的AIOConn清理
		this.waitgroup.Wait()

		//所有的待处理的AIO在此时已经被投递到completeQueue，可以关闭completeQueue。
		this.completeQueue.close()
	})
}

var defalutService *AIOService
var createOnce sync.Once

func Bind(conn net.Conn, key interface{}) (*AIOConn, error) {
	createOnce.Do(func() {
		defalutService = NewAIOService(DefaultWorkerCount)
	})
	return defalutService.Bind(conn, key)
}

func GetCompleteStatus() (*AIOConn, []byte, int, interface{}, error) {
	createOnce.Do(func() {
		defalutService = NewAIOService(DefaultWorkerCount)
	})
	return defalutService.GetCompleteStatus()
}
