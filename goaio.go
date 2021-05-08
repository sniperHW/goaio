package goaio

import (
	"errors"
	"io"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

var (
	ErrRecvTimeout        = errors.New("RecvTimeout")
	ErrSendTimeout        = errors.New("SendTimetout")
	ErrConnClosed         = errors.New("conn closed")
	ErrServiceClosed      = errors.New("service closed")
	ErrUnsupportConn      = errors.New("net.Conn does implement net.RawConn")
	ErrBusy               = errors.New("busy")
	ErrWatchFailed        = errors.New("watch failed")
	ErrActiveClose        = errors.New("active close")
	ErrCloseNone          = errors.New("close no reason")
	ErrCloseGC            = errors.New("close by gc")
	ErrCloseServiceClosed = errors.New("close because of service closed")
	ErrEmptyBuff          = errors.New("send buffs is empty")
)

const (
	EV_READ      = int(1 << 1)
	EV_WRITE     = int(1 << 2)
	EV_ERROR     = int(1 << 3)
	MaxIovecSize = 64
)

var (
	CompleteQueueSize   = 65535
	TaskQueueSize       = 65535
	ConnMgrSize         = 263
	DefaultWorkerCount  = 1
	DefaultRecvBuffSize = 4096
)

type ShareBuffer interface {
	Acquire() []byte
	Release([]byte)
}

type AIOConnOption struct {
	SendqueSize int
	RecvqueSize int
	ShareBuff   ShareBuffer
	UserData    interface{}
}

type aioContext struct {
	buffs      [][]byte
	index      int //buffs索引
	offset     int //[]byte内下标
	transfered int //已经传输的字节数
	readfull   bool
	context    interface{}
	deadline   time.Time
}

type aioContextQueue struct {
	head  int
	tail  int
	queue []aioContext
}

type AIOResult struct {
	Conn          *AIOConn
	Buffs         [][]byte
	Context       interface{}
	Err           error
	Bytestransfer int
}

type completetionQueue struct {
	mu        sync.Mutex
	cond      *sync.Cond
	head      int
	tail      int
	queue     []AIOResult
	closed    bool
	waitCount int
}

type AIOConn struct {
	fd            int
	rawconn       net.Conn
	muR           sync.Mutex
	muW           sync.Mutex
	readable      bool
	readableVer   int
	writeable     bool
	writeableVer  int
	w             *aioContextQueue
	r             *aioContextQueue
	doingW        bool
	doingR        bool
	rtimer        *time.Timer
	dorTimer      *time.Timer
	wtimer        *time.Timer
	dowTimer      *time.Timer
	service       *AIOService
	closed        int32
	pollerVersion int32
	closeOnce     sync.Once
	sendTimeout   time.Duration
	recvTimeout   time.Duration
	reason        error
	sharebuff     ShareBuffer
	userData      interface{}
	pprev         *AIOConn
	nnext         *AIOConn
	ioCount       int
	send_iovec    [MaxIovecSize]syscall.Iovec
	recv_iovec    [MaxIovecSize]syscall.Iovec
	connMgr       *connMgr
	tq            chan func()
}

type AIOService struct {
	sync.Mutex
	completeQueue chan AIOResult
	tq            chan func()
	poller        pollerI
	closed        int32
	waitgroup     *sync.WaitGroup
	closeOnce     sync.Once
	connMgr       []*connMgr
}

type connMgr struct {
	sync.Mutex
	head   AIOConn
	closed bool
}

var defalutService *AIOService

var createOnce sync.Once

func newAioContextQueue(max int) *aioContextQueue {
	if max <= 0 {
		max = 32
	}
	return &aioContextQueue{
		queue: make([]aioContext, max+1, max+1),
	}
}

func (this *aioContextQueue) empty() bool {
	return this.head == this.tail
}

func (this *aioContextQueue) add(c aioContext) error {
	if next := (this.tail + 1) % len(this.queue); next == this.head {
		return ErrBusy
	} else {
		this.queue[this.tail] = c
		this.tail = next
		return nil
	}
}

func (this *aioContextQueue) dropLast() {
	if this.head != this.tail {
		this.queue[this.tail] = aioContext{}
		if this.tail == 0 {
			this.tail = len(this.queue) - 1
		} else {
			this.tail--
		}
	}
}

func (this *aioContextQueue) front() *aioContext {
	return &this.queue[this.head]
}

func (this *aioContextQueue) popFront() {
	if this.head != this.tail {
		this.queue[this.head] = aioContext{}
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

func (this *aioContextQueue) packIovec(iovec *[MaxIovecSize]syscall.Iovec) (int, int) {
	if this.empty() {
		return 0, 0
	} else {
		cc := 0
		total := 0
		ctx := &this.queue[this.head]
		for j := ctx.index; j < len(ctx.buffs) && cc < len(*iovec); j++ {
			buff := ctx.buffs[ctx.index]
			size := len(buff) - ctx.offset
			(*iovec)[cc] = syscall.Iovec{&buff[ctx.offset], uint64(size)}
			total += size
			cc++
		}
		return cc, total
	}
}

func (this *AIOConn) GetUserData() interface{} {
	return this.userData
}

func (this *AIOConn) Close(reason error) {
	this.closeOnce.Do(func() {
		runtime.SetFinalizer(this, nil)

		if nil == reason {
			reason = ErrCloseNone
		}
		this.reason = reason

		atomic.StoreInt32(&this.closed, 1)

		this.muR.Lock()
		if nil != this.rtimer {
			this.rtimer.Stop()
			this.rtimer = nil
		}

		if !this.doingR {
			this.doingR = true
			this.tq <- this.doRead
		}
		this.muR.Unlock()

		this.muW.Lock()
		if nil != this.wtimer {
			this.wtimer.Stop()
			this.wtimer = nil
		}

		if !this.doingW {
			this.doingW = true
			this.tq <- this.doWrite
		}
		this.muW.Unlock()

		this.service.unwatch(this)

		this.rawconn.Close()

	})
}

func (this *AIOConn) processReadTimeout() {
	now := time.Now()
	if this.recvTimeout > 0 {
		for !this.r.empty() {
			f := this.r.front()
			if now.After(f.deadline) {
				this.service.postCompleteStatus(this, f.buffs, 0, ErrRecvTimeout, f.context)
				this.r.popFront()
			} else {
				break
			}
		}
	}

	var deadline time.Time

	if this.recvTimeout > 0 && !this.r.empty() && this.r.front().deadline.After(deadline) {
		deadline = this.r.front().deadline
	}

	if !deadline.IsZero() {
		this.rtimer = time.AfterFunc(now.Sub(deadline), this.onReadTimeout)
	} else {
		this.rtimer = nil
	}
}

func (this *AIOConn) processWriteTimeout() {
	now := time.Now()

	if this.sendTimeout > 0 {
		for !this.w.empty() {
			f := this.w.front()
			if now.After(f.deadline) {
				this.service.postCompleteStatus(this, f.buffs, f.transfered, ErrSendTimeout, f.context)
				this.w.popFront()
			} else {
				break
			}
		}
	}

	var deadline time.Time

	if this.sendTimeout > 0 && !this.w.empty() && this.w.front().deadline.After(deadline) {
		deadline = this.w.front().deadline
	}

	if !deadline.IsZero() {
		this.wtimer = time.AfterFunc(now.Sub(deadline), this.onWriteTimeout)
	} else {
		this.wtimer = nil
	}

}

func (this *AIOConn) onReadTimeout() {
	this.muR.Lock()
	defer this.muR.Unlock()
	this.dorTimer = this.rtimer
	if nil != this.dorTimer && !this.doingR {
		this.doingR = true
		this.tq <- this.doRead
	}
}

func (this *AIOConn) onWriteTimeout() {
	this.muW.Lock()
	defer this.muW.Unlock()
	this.dowTimer = this.wtimer
	if nil != this.dowTimer && !this.doingW {
		this.doingW = true
		this.tq <- this.doWrite
	}
}

func (this *AIOConn) SetRecvTimeout(timeout time.Duration) {
	this.muR.Lock()
	defer this.muR.Unlock()
	if atomic.LoadInt32(&this.closed) == 0 {
		if nil != this.rtimer {
			this.rtimer.Stop()
			this.rtimer = nil
		}
		this.recvTimeout = timeout
		if timeout != 0 {
			deadline := time.Now().Add(timeout)
			if this.r.setDeadline(deadline) {
				this.rtimer = time.AfterFunc(timeout, this.onReadTimeout)
			}
		} else {
			this.r.setDeadline(time.Time{})
		}
	}
}

func (this *AIOConn) SetSendTimeout(timeout time.Duration) {
	this.muW.Lock()
	defer this.muW.Unlock()
	if atomic.LoadInt32(&this.closed) == 0 {
		if nil != this.wtimer {
			this.wtimer.Stop()
			this.wtimer = nil
		}
		this.sendTimeout = timeout
		if timeout != 0 {
			deadline := time.Now().Add(timeout)
			if this.w.setDeadline(deadline) {
				this.wtimer = time.AfterFunc(timeout, this.onWriteTimeout)
			}
		} else {
			this.w.setDeadline(time.Time{})
		}
	}
}

func (this *AIOConn) Send(context interface{}, buffs ...[]byte) error {

	var deadline time.Time

	if 0 != this.sendTimeout {
		deadline = time.Now().Add(this.sendTimeout)
	}

	this.muW.Lock()

	if atomic.LoadInt32(&this.closed) == 1 {
		this.muW.Unlock()
		return ErrConnClosed
	}

	if err := this.w.add(aioContext{
		buffs:    buffs,
		context:  context,
		deadline: deadline,
	}); nil != err {
		this.muW.Unlock()
		return err
	}

	if !this.connMgr.addIO(this) {
		this.w.dropLast()
		this.muW.Unlock()
		return ErrServiceClosed
	}

	if !deadline.IsZero() && nil == this.wtimer {
		this.wtimer = time.AfterFunc(this.sendTimeout, this.onWriteTimeout)
	}

	if this.writeable && !this.doingW {
		this.doingW = true
		this.muW.Unlock()
		this.tq <- this.doWrite
	} else {
		this.muW.Unlock()
	}

	return nil

}

func (this *AIOConn) recv(context interface{}, readfull bool, buffs ...[]byte) error {
	var deadline time.Time

	if 0 != this.recvTimeout {
		deadline = time.Now().Add(this.recvTimeout)
	}

	this.muR.Lock()

	if atomic.LoadInt32(&this.closed) == 1 {
		this.muR.Unlock()
		return ErrConnClosed
	}

	if err := this.r.add(aioContext{
		buffs:    buffs,
		context:  context,
		deadline: deadline,
		readfull: readfull,
	}); nil != err {
		this.muR.Unlock()
		return err
	}

	if !this.connMgr.addIO(this) {
		this.r.dropLast()
		this.muR.Unlock()
		return ErrServiceClosed
	}

	if !deadline.IsZero() && nil == this.rtimer {
		this.rtimer = time.AfterFunc(this.recvTimeout, this.onReadTimeout)
	}

	if this.readable && !this.doingR {
		this.doingR = true
		this.muR.Unlock()
		this.tq <- this.doRead
	} else {
		this.muR.Unlock()
	}

	return nil
}

func (this *AIOConn) Recv(context interface{}, buffs ...[]byte) error {
	return this.recv(context, false, buffs...)
	/*var deadline time.Time

	if 0 != this.recvTimeout {
		deadline = time.Now().Add(this.recvTimeout)
	}

	this.muR.Lock()

	if atomic.LoadInt32(&this.closed) == 1 {
		this.muR.Unlock()
		return ErrConnClosed
	}

	if err := this.r.add(aioContext{
		buffs:    buffs,
		context:  context,
		deadline: deadline,
	}); nil != err {
		this.muR.Unlock()
		return err
	}

	if !this.connMgr.addIO(this) {
		this.r.dropLast()
		this.muR.Unlock()
		return ErrServiceClosed
	}

	if !deadline.IsZero() && nil == this.rtimer {
		this.rtimer = time.AfterFunc(this.recvTimeout, this.onReadTimeout)
	}

	if this.readable && !this.doingR {
		this.doingR = true
		this.muR.Unlock()
		this.tq <- this.doRead
	} else {
		this.muR.Unlock()
	}

	return nil*/
}

func (this *AIOConn) RecvFull(context interface{}, buffs ...[]byte) error {
	return this.recv(context, true, buffs...)
	/*var deadline time.Time

	if 0 != this.recvTimeout {
		deadline = time.Now().Add(this.recvTimeout)
	}

	this.muR.Lock()

	if atomic.LoadInt32(&this.closed) == 1 {
		this.muR.Unlock()
		return ErrConnClosed
	}

	if err := this.r.add(aioContext{
		buffs:    buffs,
		context:  context,
		deadline: deadline,
		readfull: true,
	}); nil != err {
		this.muR.Unlock()
		return err
	}

	if !this.connMgr.addIO(this) {
		this.r.dropLast()
		this.muR.Unlock()
		return ErrServiceClosed
	}

	if !deadline.IsZero() && nil == this.rtimer {
		this.rtimer = time.AfterFunc(this.recvTimeout, this.onReadTimeout)
	}

	if this.readable && !this.doingR {
		this.doingR = true
		this.muR.Unlock()
		this.tq <- this.doRead
	} else {
		this.muR.Unlock()
	}

	return nil*/
}

func (this *AIOConn) onActive(ev int) {

	if atomic.LoadInt32(&this.closed) == 1 {
		return
	}

	if ev&EV_READ != 0 || ev&EV_ERROR != 0 {
		this.muR.Lock()
		this.readable = true
		this.readableVer++
		if !this.r.empty() && !this.doingR {
			this.doingR = true
			this.muR.Unlock()
			this.tq <- this.doRead
		} else {
			this.muR.Unlock()
		}
	}

	if ev&EV_WRITE != 0 || ev&EV_ERROR != 0 {
		this.muW.Lock()
		this.writeable = true
		this.writeableVer++
		if !this.w.empty() && !this.doingW {
			this.doingW = true
			this.muW.Unlock()
			this.tq <- this.doWrite
		} else {
			this.muW.Unlock()
		}
	}
}

func (this *AIOConn) doRead() {

	this.muR.Lock()

	for atomic.LoadInt32(&this.closed) == 0 && this.readable && !this.r.empty() {

		c := this.r.front()
		ver := this.readableVer

		this.muR.Unlock()

		userShareBuffer := false
		var sharebuff []byte
		var cc int
		var total int
		if len(c.buffs) == 0 {
			if nil != this.sharebuff {
				sharebuff = this.sharebuff.Acquire()
				this.recv_iovec[0] = syscall.Iovec{&sharebuff[0], uint64(len(sharebuff))}
				userShareBuffer = true
				cc = 1
			} else {
				c.buffs = append(c.buffs, make([]byte, DefaultRecvBuffSize))
			}
		}

		if !userShareBuffer {
			cc, total = this.r.packIovec(&this.recv_iovec)
			if 0 == total {
				c := this.r.front()
				this.service.postCompleteStatus(this, c.buffs, c.transfered, ErrEmptyBuff, c.context)
				this.r.popFront()
				continue
			}
		}

		var (
			r uintptr
			e syscall.Errno
		)

		r, _, e = syscall.RawSyscall(syscall.SYS_READV, uintptr(this.fd), uintptr(unsafe.Pointer(&this.recv_iovec[0])), uintptr(cc))
		size := int(r)

		this.muR.Lock()

		if e == syscall.EINTR {
			continue
		} else if size == 0 || (e != 0 && e != syscall.EAGAIN) {

			var err error
			if size == 0 {
				err = io.EOF
			} else {
				err = e
			}

			if userShareBuffer {
				this.sharebuff.Release(sharebuff)
			}

			for !this.r.empty() {
				c := this.r.front()
				this.service.postCompleteStatus(this, c.buffs, 0, err, c.context)
				this.r.popFront()
			}

		} else if e == syscall.EAGAIN {
			if ver == this.readableVer {
				this.readable = false
			}

			if userShareBuffer {
				this.sharebuff.Release(sharebuff)
			}

		} else {

			if userShareBuffer {
				c.buffs = append(c.buffs, sharebuff)
			}

			if !userShareBuffer && c.readfull {

				remain := size
				for remain > 0 {
					s := len(c.buffs[c.index][c.offset:])
					if remain >= s {
						remain -= s
						c.transfered += s
						c.index++
						c.offset = 0
					} else {
						c.offset += remain
						c.transfered += remain
						remain = 0
					}
				}

				if c.index >= len(c.buffs) {
					this.service.postCompleteStatus(this, c.buffs, c.transfered, nil, c.context)
					this.r.popFront()
				}

			} else {

				this.service.postCompleteStatus(this, c.buffs, size, nil, c.context)
				this.r.popFront()

				if size < total && ver == this.readableVer {
					this.readable = false
				}

			}
		}
	}

	if atomic.LoadInt32(&this.closed) == 1 {
		for !this.r.empty() {
			c := this.r.front()
			this.service.postCompleteStatus(this, c.buffs, 0, this.reason, c.context)
			this.r.popFront()
		}
	} else if nil != this.dorTimer && this.rtimer == this.dorTimer {
		this.dorTimer = nil
		this.processReadTimeout()
	}

	this.doingR = false
	this.muR.Unlock()
}

func (this *AIOConn) doWrite() {

	this.muW.Lock()

	for atomic.LoadInt32(&this.closed) == 0 && this.writeable && !this.w.empty() {

		cc, total := this.w.packIovec(&this.send_iovec)
		if 0 == total {
			c := this.w.front()
			this.service.postCompleteStatus(this, c.buffs, c.transfered, ErrEmptyBuff, c.context)
			this.w.popFront()
			continue
		}

		ver := this.writeableVer
		this.muW.Unlock()

		var (
			r uintptr
			e syscall.Errno
		)

		r, _, e = syscall.RawSyscall(syscall.SYS_WRITEV, uintptr(this.fd), uintptr(unsafe.Pointer(&this.send_iovec[0])), uintptr(cc))
		size := int(r)

		this.muW.Lock()

		if e == syscall.EINTR {
			continue
		} else if size == 0 || (e != 0 && e != syscall.EAGAIN) {

			var err error
			if size == 0 {
				err = io.ErrUnexpectedEOF
			} else {
				err = e
			}

			for !this.w.empty() {
				c := this.w.front()
				this.service.postCompleteStatus(this, c.buffs, c.transfered, err, c.context)
				this.w.popFront()
			}
		} else if e == syscall.EAGAIN {
			if ver == this.writeableVer {
				this.writeable = false
				this.service.poller.enableWrite(this)
			}
		} else {
			remain := size
			c := this.w.front()
			for remain > 0 {
				s := len(c.buffs[c.index][c.offset:])
				if remain >= s {
					remain -= s
					c.transfered += s
					c.index++
					c.offset = 0
				} else {
					c.offset += remain
					c.transfered += remain
					remain = 0
				}
			}

			if c.index >= len(c.buffs) {
				this.service.postCompleteStatus(this, c.buffs, c.transfered, nil, c.context)
				this.w.popFront()
			} else if ver == this.writeableVer {
				this.writeable = false
				this.service.poller.enableWrite(this)
			}
		}
	}

	if atomic.LoadInt32(&this.closed) == 1 {
		for !this.w.empty() {
			c := this.w.front()
			this.service.postCompleteStatus(this, c.buffs, c.transfered, this.reason, c.context)
			this.w.popFront()
		}
	} else if nil != this.dowTimer && this.wtimer == this.dowTimer {
		this.dowTimer = nil
		this.processWriteTimeout()
	}

	this.doingW = false
	this.muW.Unlock()
}

func (this *connMgr) addIO(c *AIOConn) bool {
	this.Lock()
	defer this.Unlock()
	if !this.closed {
		c.ioCount++
		if c.ioCount == 1 {
			next := this.head.nnext
			c.nnext = next
			c.pprev = &this.head
			this.head.nnext = c
			if next != &this.head {
				next.pprev = c
			}
		}
		return true
	} else {
		return false
	}
}

func (this *connMgr) subIO(c *AIOConn) {
	this.Lock()
	defer this.Unlock()
	c.ioCount--
	if 0 == c.ioCount {
		prev := c.pprev
		next := c.nnext
		prev.nnext = next
		next.pprev = prev
		c.pprev = nil
		c.nnext = nil
	}
}

func (this *connMgr) close() {
	this.Lock()
	this.closed = true
	conns := []*AIOConn{}
	n := this.head.nnext
	for ; n != &this.head; n = n.nnext {
		conns = append(conns, n)
	}
	this.Unlock()

	for _, v := range conns {
		v.Close(ErrCloseServiceClosed)
	}

	for !func() bool {
		this.Lock()
		defer this.Unlock()
		if this.head.nnext == &this.head {
			return true
		} else {
			return false
		}

	}() {
		runtime.Gosched()
	}

}

func NewAIOService(worker int) *AIOService {
	if poller, err := openPoller(); nil == err {
		waitgroup := &sync.WaitGroup{}
		s := &AIOService{}
		s.completeQueue = make(chan AIOResult, CompleteQueueSize)
		s.poller = poller
		s.connMgr = make([]*connMgr, ConnMgrSize)
		s.waitgroup = waitgroup
		s.tq = make(chan func(), TaskQueueSize)
		for k, _ := range s.connMgr {
			m := &connMgr{}
			m.head.nnext = &m.head
			s.connMgr[k] = m
		}

		if worker <= 0 {
			worker = 1
		}

		for i := 0; i < worker; i++ {
			go func() {
				waitgroup.Add(1)
				defer waitgroup.Done()
				for {
					v, ok := <-s.tq
					if !ok {
						return
					} else {
						v()
					}
				}
			}()
		}

		go poller.wait(&s.closed)

		runtime.SetFinalizer(s, func(s *AIOService) {
			s.Close()
		})

		return s
	} else {
		return nil
	}
}

func (this *AIOService) unwatch(c *AIOConn) {
	this.poller.unwatch(c)
}

func (this *AIOService) Bind(conn net.Conn, option AIOConnOption) (*AIOConn, error) {

	this.Lock()
	defer this.Unlock()

	if atomic.LoadInt32(&this.closed) == 1 {
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
		rawconn:   conn,
		service:   this,
		sharebuff: option.ShareBuff,
		r:         newAioContextQueue(option.RecvqueSize),
		w:         newAioContextQueue(option.SendqueSize),
		userData:  option.UserData,
		tq:        this.tq,
		connMgr:   this.connMgr[fd%len(this.connMgr)],
	}

	if this.poller.watch(cc) {
		runtime.SetFinalizer(cc, func(cc *AIOConn) {
			cc.Close(ErrCloseGC)
		})
		return cc, nil
	} else {
		return nil, ErrWatchFailed
	}
}

func (this *AIOService) postCompleteStatus(c *AIOConn, buff [][]byte, bytestransfer int, err error, context interface{}) {
	this.completeQueue <- AIOResult{
		Conn:          c,
		Context:       context,
		Err:           err,
		Buffs:         buff,
		Bytestransfer: bytestransfer,
	}
	c.connMgr.subIO(c)
}

func (this *AIOService) GetCompleteStatus() (r AIOResult, ok bool) {
	r, ok = <-this.completeQueue
	return
}

func (this *AIOService) Close() {
	this.closeOnce.Do(func() {
		runtime.SetFinalizer(this, nil)
		this.Lock()
		defer this.Unlock()
		atomic.StoreInt32(&this.closed, 1)

		this.poller.close()
		for _, v := range this.connMgr {
			v.close()
		}

		close(this.tq)
		//等待worker处理完所有的AIOConn清理
		this.waitgroup.Wait()
		//所有的待处理的AIO在此时已经被投递到completeQueue，可以关闭completeQueue。
		close(this.completeQueue)
	})
}

func Bind(conn net.Conn, option AIOConnOption) (*AIOConn, error) {
	createOnce.Do(func() {
		defalutService = NewAIOService(DefaultWorkerCount)
	})
	return defalutService.Bind(conn, option)
}

func GetCompleteStatus() (AIOResult, bool) {
	createOnce.Do(func() {
		defalutService = NewAIOService(DefaultWorkerCount)
	})
	return defalutService.GetCompleteStatus()
}
