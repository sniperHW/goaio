package goaio

import (
	"container/list"
	"fmt"
	"net"
	"runtime"
	"sync"
	"syscall"
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
}

type aioContext struct {
	buff    []byte
	context interface{}
}

type aioResult struct {
	conn          *AIOConn
	context       interface{}
	err           error
	bytestransfer int
}

func (this *AIOConn) Close() {

}

func (this *AIOConn) canRead() bool {
	return true
}

func (this *AIOConn) canWrite() bool {
	return true
}

func (this *AIOConn) Send(buff []byte, context interface{}) error {
	this.Lock()
	defer this.Unlock()
	this.w.PushBack(aioContext{
		buff:    buff,
		context: context,
	})

	if this.writeable && !this.doing {
		this.service.pushIOTask(this)
	}

	return nil
}

func (this *AIOConn) Recv(buff []byte, context interface{}) error {
	this.Lock()
	defer this.Unlock()
	this.r.PushBack(aioContext{
		buff:    buff,
		context: context,
	})

	if this.readable && !this.doing {
		this.service.pushIOTask(this)
	}

	return nil
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

	if !this.doing {
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
		if ver != this.readableVer {
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
		if ver != this.writeableVer {
			this.writeable = false
		}
	} else {
		this.w.Remove(this.w.Front())
		this.service.postCompleteStatus(this, size, nil, c.context)
	}
}

func (this *AIOConn) Do() {
	this.Lock()
	defer this.Unlock()
	for {
		if this.closed {
			return
		} else {
			if this.canRead() {
				this.doRead()
			}

			if this.canWrite() {
				this.doWrite()
			}
		}
	}
}

type AIOService struct {
	mu     sync.Mutex
	cond   *sync.Cond
	r      *list.List
	tq     *taskQueue
	poller pollerI
}

func NewAIOService(worker int) *AIOService {
	if poller, err := openPoller(); nil == err {
		s := &AIOService{}
		s.cond = sync.NewCond(&s.mu)
		s.r = list.New()
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
	this.r.PushBack(aioResult{
		conn:          c,
		context:       context,
		err:           err,
		bytestransfer: bytestransfer,
	})
	this.mu.Unlock()
	this.cond.Signal()
}

func (this *AIOService) GetCompleteStatus() (err error, c *AIOConn, bytestransfer int, context interface{}) {
	this.mu.Lock()
	for this.r.Len() == 0 {
		this.cond.Wait()
	}

	e := this.r.Front()
	this.r.Remove(e)
	this.mu.Unlock()

	err = e.Value.(aioResult).err
	c = e.Value.(aioResult).conn
	bytestransfer = e.Value.(aioResult).bytestransfer
	context = e.Value.(aioResult).context
	return
}
