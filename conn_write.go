// +build !writev

package goaio

import (
	"io"
	"syscall"
)

type AIOConn struct {
	aioConn
}

func (this *AIOConn) doWrite() {
	c := this.w.front()
	this.Unlock()
	ver := this.writeableVer
	size, err := syscall.Write(this.fd, c.buff[c.offset:])
	this.Lock()

	if size == 0 && len(c.buff[c.offset:]) > 0 {
		err = io.ErrUnexpectedEOF
	}

	if err == syscall.EINTR {
		return
	} else if err != nil && err != syscall.EAGAIN {
		for !this.w.empty() {
			c := this.w.front()
			this.service.postCompleteStatus(this, c.buff, c.offset, err, c.context)
			this.w.popFront()
		}
	} else if err == syscall.EAGAIN {
		if ver == this.writeableVer {
			this.writeable = false
		}
	} else {
		if len(c.buff[c.offset:]) == size {
			this.service.postCompleteStatus(this, c.buff, len(c.buff), nil, c.context)
			this.w.popFront()
		} else {
			c.offset += size
			if ver == this.writeableVer {
				this.writeable = false
			}
		}
	}
}
