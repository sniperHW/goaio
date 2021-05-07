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
	EV_READ  = int(1 << 1)
	EV_WRITE = int(1 << 2)
	EV_ERROR = int(1 << 3)
)

const MaxIovecSize = 64

var (
	DefaultWorkerCount = 1
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
	sendTimeout   time.Duration
	recvTimeout   time.Duration
	timer         *time.Timer
	reason        error
	sharebuff     ShareBuffer
	userData      interface{}
	doTimeout     *time.Timer
	pprev         *AIOConn
	nnext         *AIOConn
	ioCount       int
	send_iovec    [MaxIovecSize]syscall.Iovec
	recv_iovec    [MaxIovecSize]syscall.Iovec
	connMgr       *connMgr
	tq            chan *AIOConn
}

type AIOService struct {
	sync.Mutex
	completeQueue chan AIOResult
	tq            []chan *AIOConn
	poller        pollerI
	closed        *int32
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
	if (this.tail+1)%len(this.queue) == this.head {
		return ErrBusy
	} else {
		this.queue[this.tail] = c
		this.tail = (this.tail + 1) % len(this.queue)
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
		this.Lock()
		defer this.Unlock()
		this.closed = true
		if nil == reason {
			reason = ErrCloseNone
		}
		this.reason = reason

		if nil != this.timer {
			this.timer.Stop()
			this.timer = nil
		}
		if !this.doing {
			this.doing = true
			this.tq <- this
		}
	})
}

func (this *AIOConn) processTimeout() {
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
	if this.recvTimeout > 0 && !this.r.empty() && this.r.front().deadline.After(deadline) {
		deadline = this.r.front().deadline
	}

	if this.sendTimeout > 0 && !this.w.empty() && this.w.front().deadline.After(deadline) {
		deadline = this.w.front().deadline
	}

	if !deadline.IsZero() {
		this.timer = time.AfterFunc(now.Sub(deadline), this.onTimeout)
	} else {
		this.timer = nil
	}
}

func (this *AIOConn) onTimeout() {
	this.Lock()
	defer this.Unlock()
	this.doTimeout = this.timer
	if nil != this.doTimeout && !this.doing {
		this.doing = true
		this.tq <- this
	}
}

func (this *AIOConn) SetRecvTimeout(timeout time.Duration) {
	this.Lock()
	defer this.Unlock()
	if !this.closed {
		if nil != this.timer {
			this.timer.Stop()
			this.timer = nil
		}
		this.recvTimeout = timeout
		if timeout != 0 {
			deadline := time.Now().Add(timeout)
			if this.r.setDeadline(deadline) {
				this.timer = time.AfterFunc(timeout, this.onTimeout)
			}
		} else {
			this.r.setDeadline(time.Time{})
		}
	}
}

func (this *AIOConn) SetSendTimeout(timeout time.Duration) {
	this.Lock()
	defer this.Unlock()
	if !this.closed {
		if nil != this.timer {
			this.timer.Stop()
			this.timer = nil
		}
		this.sendTimeout = timeout
		if timeout != 0 {
			deadline := time.Now().Add(timeout)
			if this.w.setDeadline(deadline) {
				this.timer = time.AfterFunc(timeout, this.onTimeout)
			}
		} else {
			this.w.setDeadline(time.Time{})
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

func (this *AIOConn) Send(context interface{}, buffs ...[]byte) error {
	this.Lock()
	defer this.Unlock()

	if this.closed {
		return ErrConnClosed
	} else {

		var deadline time.Time

		timeout := this.getSendTimeout()

		if 0 != timeout {
			deadline = time.Now().Add(timeout)
		}

		if err := this.w.add(aioContext{
			buffs:    buffs,
			context:  context,
			deadline: deadline,
		}); nil != err {
			return err
		}

		if !this.connMgr.addIO(this) {
			this.w.dropLast()
			return ErrServiceClosed
		}

		if !deadline.IsZero() && nil == this.timer {
			this.timer = time.AfterFunc(timeout, this.onTimeout)
		}

		if this.writeable && !this.doing {
			this.doing = true
			this.tq <- this
		}

		return nil
	}
}

func (this *AIOConn) Recv(context interface{}, buffs ...[]byte) error {
	this.Lock()
	defer this.Unlock()

	if this.closed {
		return ErrConnClosed
	} else {

		var deadline time.Time

		timeout := this.getRecvTimeout()

		if 0 != timeout {
			deadline = time.Now().Add(timeout)
		}

		if err := this.r.add(aioContext{
			buffs:    buffs,
			context:  context,
			deadline: deadline,
		}); nil != err {
			return err
		}

		if !this.connMgr.addIO(this) {
			this.r.dropLast()
			return ErrServiceClosed
		}

		if !deadline.IsZero() && nil == this.timer {
			this.timer = time.AfterFunc(timeout, this.onTimeout)
		}

		if this.readable && !this.doing {
			this.doing = true
			this.tq <- this
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
	}

	if (this.canRead() || this.canWrite()) && !this.doing {
		this.doing = true
		this.tq <- this
	}
}

func (this *AIOConn) doRead() {
	c := this.r.front()
	ver := this.readableVer

	this.Unlock()

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
			total = len(sharebuff)
		} else {
			c.buffs = append(c.buffs, make([]byte, 4096))
		}
	}

	if !userShareBuffer {
		cc, total = this.r.packIovec(&this.recv_iovec)
	}

	var (
		r uintptr
		e syscall.Errno
	)

	r, _, e = syscall.Syscall(syscall.SYS_READV, uintptr(this.fd), uintptr(unsafe.Pointer(&this.recv_iovec[0])), uintptr(cc))
	size := int(r)

	this.Lock()

	if e == syscall.EINTR {
		return
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

		this.service.postCompleteStatus(this, c.buffs, size, nil, c.context)
		this.r.popFront()

		if size < total && ver == this.readableVer {
			this.readable = false
		}

	}
}

func (this *AIOConn) doWrite() {
	cc, total := this.w.packIovec(&this.send_iovec)
	if 0 == total {
		c := this.w.front()
		this.service.postCompleteStatus(this, c.buffs, c.transfered, ErrEmptyBuff, c.context)
		this.w.popFront()
		return
	}

	ver := this.writeableVer
	this.Unlock()

	var (
		r uintptr
		e syscall.Errno
	)

	r, _, e = syscall.Syscall(syscall.SYS_WRITEV, uintptr(this.fd), uintptr(unsafe.Pointer(&this.send_iovec[0])), uintptr(cc))
	size := int(r)

	this.Lock()

	if e == syscall.EINTR {
		return
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

func (this *AIOConn) do() {
	this.Lock()
	defer this.Unlock()
	for {
		if this.closed {
			for !this.r.empty() {
				c := this.r.front()
				this.service.postCompleteStatus(this, c.buffs, 0, this.reason, c.context)
				this.r.popFront()
			}
			for !this.w.empty() {
				c := this.w.front()
				this.service.postCompleteStatus(this, c.buffs, c.transfered, this.reason, c.context)
				this.w.popFront()
			}
			this.service.unwatch(this)
			this.rawconn.Close()
			return
		} else {

			if nil != this.doTimeout && this.timer == this.doTimeout {
				this.doTimeout = nil
				this.processTimeout()
			}

			if this.canRead() {
				this.doRead()
			}

			if this.canWrite() {
				this.doWrite()
			}

			if !(this.closed || this.canRead() || this.canWrite() || nil != this.doTimeout && this.timer == this.doTimeout) {
				break
			}
		}
	}

	this.doing = false
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
		s.completeQueue = make(chan AIOResult, 65535)
		s.poller = poller
		s.connMgr = make([]*connMgr, 251)
		s.waitgroup = waitgroup
		s.closed = new(int32)
		for k, _ := range s.connMgr {
			m := &connMgr{}
			m.head.nnext = &m.head
			s.connMgr[k] = m
		}

		if worker <= 0 {
			worker = 1
		}

		for i := 0; i < worker; i++ {
			tq := make(chan *AIOConn, 4096)
			s.tq = append(s.tq, tq)
			go func() {
				waitgroup.Add(1)
				defer waitgroup.Done()
				for {
					v, ok := <-tq
					if !ok {
						return
					} else {
						v.do()
					}
				}
			}()
		}

		go poller.wait(s.closed)

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

	if 1 == *this.closed {
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
		tq:        this.tq[fd%len(this.tq)],
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
	c.connMgr.subIO(c)
	this.completeQueue <- AIOResult{
		Conn:          c,
		Context:       context,
		Err:           err,
		Buffs:         buff,
		Bytestransfer: bytestransfer,
	}
}

func (this *AIOService) GetCompleteStatus() (r AIOResult, err error) {
	ok := false
	r, ok = <-this.completeQueue
	if !ok {
		err = ErrServiceClosed
	}
	return
}

func (this *AIOService) Close() {
	this.closeOnce.Do(func() {
		runtime.SetFinalizer(this, nil)
		this.Lock()
		defer this.Unlock()
		atomic.StoreInt32(this.closed, 1)
		this.poller.close()
		for _, v := range this.connMgr {
			v.close()
		}

		for _, v := range this.tq {
			close(v)
		}

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

func GetCompleteStatus() (AIOResult, error) {
	createOnce.Do(func() {
		defalutService = NewAIOService(DefaultWorkerCount)
	})
	return defalutService.GetCompleteStatus()
}
