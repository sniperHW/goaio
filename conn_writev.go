// +build writev

package goaio

import (
	"io"
	"syscall"
	"unsafe"
)

const MaxSendSize = 1024 * 256
const MaxSendIovecSize = 64

type AIOConn struct {
	aioConn
	send_iovec [MaxSendIovecSize]syscall.Iovec
}

func (this *aioContextQueue) packIovec(iovec *[MaxSendIovecSize]syscall.Iovec) (int, int) {
	if this.empty() {
		return 0, 0
	} else {
		cc := 0
		total := 0
		for i := this.head; ; {
			b := &this.queue[i]
			size := len(b.buff) - b.offset
			(*iovec)[cc] = syscall.Iovec{&b.buff[b.offset], uint64(size)}
			total += size
			cc++
			i = (i + 1) % len(this.queue)
			if i == this.tail || total >= MaxSendSize || cc >= len(*iovec) {
				break
			}
		}
		return cc, total
	}
}

func (this *AIOConn) doWrite() {
	ver := this.writeableVer
	cc, total := this.w.packIovec(&this.send_iovec)
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
	} else if (size == 0 && total > 0) || (e != 0 && e != syscall.EAGAIN) {

		var err error
		if size == 0 {
			err = io.ErrUnexpectedEOF
		} else {
			err = e
		}

		for !this.w.empty() {
			c := this.w.front()
			this.service.postCompleteStatus(this, c.buff, c.offset, err, c.context)
			this.w.popFront()
		}
	} else if e == syscall.EAGAIN {
		if ver == this.writeableVer {
			this.writeable = false
			this.service.poller.enableWrite(this)
		}
	} else {
		remain := size
		for remain > 0 {
			c := this.w.front()
			if remain >= len(c.buff[c.offset:]) {
				this.service.postCompleteStatus(this, c.buff, len(c.buff), nil, c.context)
				remain -= len(c.buff[c.offset:])
				this.w.popFront()
			} else {
				c.offset += remain
				remain = 0
			}
		}

		if size < total {
			if ver == this.writeableVer {
				this.writeable = false
				this.service.poller.enableWrite(this)
			}
		}
	}
}
