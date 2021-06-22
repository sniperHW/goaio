//go test -covermode=count -v -coverprofile=coverage.out -run=.
//go tool cover -html=coverage.out
//ulimit -n 1048576
//go test -v -run=^$ -bench BenchmarkEcho128KParallel -count 100
package goaio

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"github.com/stretchr/testify/assert"
	"io"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type TestBufferPool struct {
	pool chan []byte
}

func NewBufferPool(bufsize int) *TestBufferPool {
	size := 1
	p := &TestBufferPool{
		pool: make(chan []byte, size),
	}
	for i := 0; i < size; i++ {
		p.pool <- make([]byte, bufsize)
	}
	return p
}

func (p *TestBufferPool) Acquire() []byte {
	return <-p.pool
}

func (p *TestBufferPool) Release(buff []byte) {
	p.pool <- buff[:cap(buff)]
}

func init() {

	go http.ListenAndServe(":6060", nil)
}

const (
	bufSize = 65536
)

var resultCount int32

func echoServer(t testing.TB, bufsize int) (net.Listener, chan struct{}) {

	tcpAddr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		panic(err.Error())
	}

	ln, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		panic(err.Error())
	}

	w := NewAIOService(1)

	die := make(chan struct{})

	var clientCount int32

	go func() {
		for {
			res, ok := w.GetCompleteStatus()
			if !ok {
				break
			} else {
				if res.Err != nil {
					res.Conn.Close(err)
					atomic.AddInt32(&clientCount, -1)
				} else if res.Context.(rune) == 'r' {
					res.Conn.AsynSend('w', res.Buff[:res.Bytestransfer], -1)
				} else {
					res.Conn.AsynRecv('r', res.Buff[:cap(res.Buff)], -1)
				}
			}

			if 0 == atomic.LoadInt32(&clientCount) {
				break
			}
		}
		w.Close()
		close(die)
	}()

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				if ne, ok := err.(net.Error); ok && ne.Temporary() {
					fmt.Printf("accept temp err: %v\n", ne)
					continue
				} else {
					return
				}
			}

			c, err := w.CreateAIOConn(conn, AIOConnOption{})
			if err != nil {
				panic(err)
				w.Close()
				return
			}

			atomic.AddInt32(&clientCount, 1)

			buff := make([]byte, bufsize)
			if err := c.AsynRecv('r', buff, -1); nil != err {
				fmt.Println("first recv", err, "fd", c.fd)
				panic("panic")
			}
		}
	}()
	return ln, die
}

func TestDefault(t *testing.T) {
	ln, serverDie := echoServer(t, 4096)

	defer func() {
		<-serverDie
		ln.Close()
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	c, err := CreateAIOConn(conn, AIOConnOption{})

	if nil != err {
		t.Fatal(err)
	}

	wx := make([]byte, 4096)

	c.Send('w', wx, -1)

	for {
		res, ok := GetCompleteStatus()
		if !ok {
			break
		} else if nil == res.Err {
			if res.Context.(rune) == 'w' {
				fmt.Println("here")
				res.Conn.Close(ErrActiveClose)
				break
			}
		}
	}
}

func TestRecvFull(t *testing.T) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		panic(err.Error())
	}

	ln, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		panic(err.Error())
	}

	die := make(chan struct{})

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}

			c, _ := CreateAIOConn(conn, AIOConnOption{})

			c.RecvFull(nil, make([]byte, 5), -1)

			for {
				res, ok := GetCompleteStatus()
				fmt.Println(res, ok)
				if !ok {
					break
				} else if nil == res.Err {
					assert.Equal(t, string(res.Buff), "hello")
					res.Conn.Close(nil)
					break
				}
			}

			close(die)
			return
		}
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	conn.Write([]byte("he"))
	time.Sleep(time.Second)
	conn.Write([]byte("llo"))

	<-die
}

func TestSendEmptyBuff(t *testing.T) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		panic(err.Error())
	}

	ln, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		panic(err.Error())
	}

	die := make(chan struct{})

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				buff := make([]byte, 4096)
				for {
					n, err := conn.Read(buff)
					fmt.Println(n, err)
					conn.Close()
					close(die)
					break
				}
			}()
		}
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	c, err := CreateAIOConn(conn, AIOConnOption{})

	if nil != err {
		t.Fatal(err)
	}

	go func() {
		for {
			res, ok := GetCompleteStatus()
			if !ok {
				break
			} else if nil != res.Err {
				assert.Equal(t, res.Err, ErrEmptyBuff)
				res.Conn.Close(nil)
				break
			}
		}
	}()

	c.Send('w', nil, -1)

	<-die

	ln.Close()

}

func TestBusySend(t *testing.T) {

	tcpAddr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		panic(err.Error())
	}

	ln, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		panic(err.Error())
	}

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				buff := make([]byte, 4096)
				for {
					_, err := conn.Read(buff)
					if nil != err {
						break
					}
				}
			}()
		}
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	c, err := CreateAIOConn(conn, AIOConnOption{})

	if nil != err {
		t.Fatal(err)
	}

	sendcount := int32(0)
	sendBreak := int32(0)

	die := make(chan struct{})

	var closeOnce sync.Once

	go func() {
		for {
			_, ok := GetCompleteStatus()
			if !ok {
				break
			} else {
				if 0 == atomic.AddInt32(&sendcount, -1) && atomic.LoadInt32(&sendBreak) == 1 {
					break
				}
			}
		}
		closeOnce.Do(func() {
			close(die)
		})
	}()

	go func() {
		for {
			atomic.AddInt32(&sendcount, 1)
			if err := c.Send('w', []byte("string"), -1); nil != err {
				if 0 == atomic.AddInt32(&sendcount, -1) {
					closeOnce.Do(func() {
						close(die)
					})
				} else {
					atomic.StoreInt32(&sendBreak, 1)
				}
				break
			}
		}
	}()

	go func() {
		time.Sleep(time.Millisecond * 5)
		c.Close(nil)
	}()

	<-die

	assert.Equal(t, 0, c.ioCount)

	ln.Close()
}

func TestRecvUseEmptyBuff(t *testing.T) {

	tcpAddr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		panic(err.Error())
	}

	ln, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		panic(err.Error())
	}

	w := NewAIOService(1)

	die := make(chan struct{})

	go func() {
		for {
			res, ok := w.GetCompleteStatus()
			if !ok {
				break
			} else {
				if res.Err != nil {

				} else if res.Context.(rune) == 'r' {
					assert.Equal(t, 5, res.Bytestransfer)
					close(die)
					return
				}
			}
		}
	}()

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				if ne, ok := err.(net.Error); ok && ne.Temporary() {
					continue
				} else {
					return
				}
			}

			c, err := w.CreateAIOConn(conn, AIOConnOption{})
			if err != nil {
				panic(err)
				w.Close()
				return
			}

			if err := c.Recv('r', nil, -1); nil != err {
				fmt.Println("first recv", err, "fd", c.fd)
				panic("panic")
			}
		}
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	c, err := w.CreateAIOConn(conn, AIOConnOption{})

	c.Send('w', []byte("hello"), -1)

	<-die

	ln.Close()

}

func TestGC(t *testing.T) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		panic(err.Error())
	}

	ln, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		panic(err.Error())
	}

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	conn.(*net.TCPConn).SetWriteBuffer(4096)

	w := NewAIOService(1)

	defer w.Close()

	_, err = w.CreateAIOConn(conn, AIOConnOption{})

	if nil != err {
		t.Fatal(err)
	}

	_ = NewAIOService(2)

	runtime.GC()

	ln.Close()
}

func TestSendBigBuff(t *testing.T) {
	ln, serverDie := echoServer(t, 128)

	defer func() {
		<-serverDie
		ln.Close()
	}()

	w := NewAIOService(1)

	defer w.Close()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	c, err := w.CreateAIOConn(conn, AIOConnOption{})

	if nil != err {
		t.Fatal(err)
	}

	wx := make([]byte, 1204*1024)

	c.Send('w', wx, -1)

	rx := make([]byte, 4096)

	c.Recv('r', rx, -1)

	defer c.Close(ErrActiveClose)

	for {
		res, ok := w.GetCompleteStatus()
		if !ok {
			break
		} else if nil == res.Err {
			if res.Context.(rune) == 'w' {
				break
			} else {
				res.Conn.Recv('r', rx, -1)
			}
		}
	}
}

func TestShareBuffer(t *testing.T) {
	ln, serverDie := echoServer(t, 4096)

	defer func() {
		<-serverDie
		ln.Close()
	}()

	w := NewAIOService(1)

	defer w.Close()

	buffpool := NewBufferPool(4096)

	for i := 0; i < 10; i++ {

		conn, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Fatal(err)
		}

		c, err := w.CreateAIOConn(conn, AIOConnOption{ShareBuff: buffpool})

		if nil != err {
			t.Fatal(err)
		}

		wx := make([]byte, 4096)

		c.Send('w', wx, -1)

		defer c.Close(ErrActiveClose)
	}

	count := 0

	for {
		res, ok := w.GetCompleteStatus()
		if !ok {
			break
		} else if nil == res.Err {
			if res.Context.(rune) == 'w' {
				//使用ShareBuff,不需要提供buff
				res.Conn.Recv('r', nil, -1)
			} else {
				//使用关闭归还buffpool供其它连接使用
				buffpool.Release(res.Buff)
				count++
				if count == 10 {
					break
				}
			}
		}
	}
}

func TestShareBuffer2(t *testing.T) {

	tcpAddr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		panic(err.Error())
	}

	ln, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		panic(err.Error())
	}

	w := NewAIOService(1)

	defer w.Close()

	buffpool := NewBufferPool(4096)

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}

			fmt.Println("on new client")

			c, _ := w.CreateAIOConn(conn, AIOConnOption{ShareBuff: buffpool})

			c.Recv('r', nil, -1)
		}
	}()

	die := make(chan struct{})

	go func() {
		for {
			res, ok := w.GetCompleteStatus()
			if !ok {
				break
			} else if nil != res.Err {
				break
			}
		}
		close(die)
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	conn.Close()
	<-die
}

func TestShareBuffer3(t *testing.T) {

	tcpAddr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		panic(err.Error())
	}

	ln, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		panic(err.Error())
	}

	w := NewAIOService(1)

	defer w.Close()

	buffpool := NewBufferPool(4096)

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}

			fmt.Println("on new client")

			c, _ := w.CreateAIOConn(conn, AIOConnOption{ShareBuff: buffpool})

			c.Recv('r', nil, -1)
		}
	}()

	die := make(chan struct{})

	go func() {
		for {
			res, ok := w.GetCompleteStatus()
			if !ok {
				break
			} else if nil != res.Err {
				break
			} else {
				buffpool.Release(res.Buff)
				res.Conn.Recv('r', nil, -1)
			}
		}
		close(die)
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	conn.Write([]byte("hello"))
	time.Sleep(time.Second)
	conn.Close()
	<-die
}

func TestRecvTimeout(t *testing.T) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		panic(err.Error())
	}

	ln, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		panic(err.Error())
	}

	clients := []net.Conn{}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			clients = append(clients, conn)
		}
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	w := NewAIOService(1)

	c, err := w.CreateAIOConn(conn, AIOConnOption{})

	if nil != err {
		t.Fatal(err)
	}

	rx := make([]byte, 4096)

	c.Recv('r', rx, time.Second)

	rx = make([]byte, 4096)

	c.Recv('r', rx, time.Second)

	count := 0

	for {
		res, ok := w.GetCompleteStatus()
		if !ok {
			break
		} else if nil != res.Err {
			assert.Equal(t, res.Err, ErrRecvTimeout)
			count++
			if count == 2 {
				res.Conn.Close(ErrActiveClose)
				break
			}
		}
	}

	ln.Close()
	w.Close()
	for _, v := range clients {
		v.Close()
	}
}

func TestSendTimeout(t *testing.T) {

	tcpAddr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		panic(err.Error())
	}

	ln, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		panic(err.Error())
	}

	clients := []net.Conn{}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			conn.(*net.TCPConn).SetReadBuffer(4096)
			clients = append(clients, conn)
		}
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	conn.(*net.TCPConn).SetWriteBuffer(4096)

	w := NewAIOService(1)

	c, err := w.CreateAIOConn(conn, AIOConnOption{})

	if nil != err {
		t.Fatal(err)
	}

	wx := make([]byte, 4096)

	c.Send('w', wx, time.Second)

	c.Send('w', wx, time.Second)

	for {
		res, ok := w.GetCompleteStatus()
		if !ok {
			break
		} else if nil != res.Err {
			if res.Err != ErrSendTimeout {
				panic("err type mismatch")
			}
			res.Conn.Close(res.Err)
			break
		} else {
			res.Conn.Send('w', wx, time.Second)
		}
	}

	ln.Close()
	w.Close()
	for _, v := range clients {
		v.Close()
	}
}

func TestEchoTiny(t *testing.T) {
	ln, serverDie := echoServer(t, 4096)

	defer func() {
		<-serverDie
		ln.Close()
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	tx := []byte("hello world")
	rx := make([]byte, len(tx))

	_, err = conn.Write(tx)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("tx:", string(tx))
	_, err = conn.Read(rx)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("rx:", string(tx))
	conn.Close()
}

func TestEchoHuge(t *testing.T) {
	ln, serverDie := echoServer(t, 4096)

	defer func() {
		<-serverDie
		ln.Close()
	}()

	tx := make([]byte, 100*1024*1024)
	n, err := io.ReadFull(rand.Reader, tx)
	if err != nil {
		t.Fatal(err)
	}

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		n, err := conn.Write(tx)
		if err != nil {
			t.Fatal(err)
		}
		t.Log("ping size", n)
	}()

	rx := make([]byte, len(tx))
	n, err = io.ReadFull(conn, rx)
	if err != nil {
		t.Fatal(err, n)
	}
	t.Log("pong size:", n)

	if bytes.Compare(tx, rx) != 0 {
		t.Fatal("incorrect receiving")
	}
	conn.Close()
}

func Test100(t *testing.T) {
	testParallel(t, 10, 1024)
}

/*func Test1k(t *testing.T) {
	testParallel(t, 1024, 1024)
}
func Test2k(t *testing.T) {
	testParallel(t, 2048, 1024)
}*/

/*func Test4k(t *testing.T) {
	testParallel(t, 4096, 1024, false)
}*/

/*
func Test8k(t *testing.T) {
	testParallel(t, 8192, 1024)
}

func Test10k(t *testing.T) {
	testParallel(t, 10240, 1024)
}

func Test12k(t *testing.T) {
	testParallel(t, 12288, 1024)
}*/

/*func Test1kTiny(t *testing.T) {
	testParallel(t, 1024, 16)
}

func Test2kTiny(t *testing.T) {
	testParallel(t, 2048, 16)
}*/

/*func Test4kTiny(t *testing.T) {
	testParallel(t, 4096, 16, false)
}*/

func testParallel(t *testing.T, par int, msgsize int) {
	t.Log("testing concurrent:", par, "connections")
	ln, serverDie := echoServer(t, msgsize)
	defer func() {
		t.Log("wait server finish")
		<-serverDie
		ln.Close()
	}()

	w := NewAIOService(1)

	die := make(chan struct{})

	ok := make(chan struct{})

	go func() {

		conns := []*AIOConn{}

		for i := 0; i < par; i++ {
			data := make([]byte, msgsize)
			conn, err := net.Dial("tcp", ln.Addr().String())

			if err != nil {
				log.Fatal(err)
			}

			c, err := w.CreateAIOConn(conn, AIOConnOption{})
			if err != nil {
				t.Fatal(err)
			}

			conns = append(conns, c)

			// send
			err = c.Send('w', data, -1)
			if err != nil {
				panic(err)
				log.Fatal(err)
			}
		}
		<-die

		for _, v := range conns {
			v.Close(ErrActiveClose)
		}

		close(ok)

	}()

	nbytes := 0
	ntotal := msgsize * par

	go func() {

		for {
			res, ok := w.GetCompleteStatus()
			if !ok {
				break
			} else {
				if nil != res.Err {
					res.Conn.Close(res.Err)
				} else if res.Context.(rune) == 'r' {
					nbytes += res.Bytestransfer
					if nbytes >= ntotal {
						t.Log("completed:", nbytes)
						close(die)
						break
					}
				} else {
					res.Conn.Recv('r', res.Buff[:cap(res.Buff)], -1)
				}
			}
		}
	}()

	<-ok
}

func BenchmarkEcho128B(b *testing.B) {
	benchmarkEcho(b, 128, 1)
}

func BenchmarkEcho1K(b *testing.B) {
	benchmarkEcho(b, 1024, 1)
}

func BenchmarkEcho4K(b *testing.B) {
	benchmarkEcho(b, 4096, 1)
}

func BenchmarkEcho64K(b *testing.B) {
	benchmarkEcho(b, 65536, 1)
}

func BenchmarkEcho128K(b *testing.B) {
	benchmarkEcho(b, 128*1024, 64)
}

/*
func BenchmarkEcho128BParallel(b *testing.B) {
	benchmarkEcho(b, 128, 128)
}

func BenchmarkEcho1KParallel(b *testing.B) {
	benchmarkEcho(b, 1024, 128)
}

func BenchmarkEcho4KParallel(b *testing.B) {
	benchmarkEcho(b, 4096, 128)
}

func BenchmarkEcho64KParallel(b *testing.B) {
	benchmarkEcho(b, 65536, 128)
}

*/
func BenchmarkEcho128KParallel(b *testing.B) {
	benchmarkEcho(b, 128*1024, 1)
}

func benchmarkEcho(b *testing.B, bufsize int, numconn int) {

	b.Log("benchmark echo with message size:", bufsize, "with", numconn, "parallel connections, for", b.N, "times")

	ln, serverDie := echoServer(b, bufsize)
	defer func() {
		<-serverDie
		ln.Close()
	}()

	w := NewAIOService(1)

	defer func() {
		w.Close()
	}()

	addr, _ := net.ResolveTCPAddr("tcp", ln.Addr().String())
	for i := 0; i < numconn; i++ {
		rx := make([]byte, bufsize)
		tx := make([]byte, bufsize)
		conn, err := net.DialTCP("tcp", nil, addr)
		if err != nil {
			b.Fatal("dial:", err)
			return
		}

		c, err := w.CreateAIOConn(conn, AIOConnOption{})
		if err != nil {
			b.Fatal(err)
		}

		c.AsynSend('w', tx, -1)
		c.AsynRecv('r', rx, -1)
		defer c.Close(ErrActiveClose)
	}

	b.ReportAllocs()
	b.SetBytes(int64(bufsize * numconn))
	b.ResetTimer()

	count := 0
	target := bufsize * b.N * numconn

	for {
		res, ok := w.GetCompleteStatus()
		if !ok {
			break
		} else if nil != res.Err {
			res.Conn.Close(res.Err)
		} else if res.Context.(rune) == 'r' {
			count += res.Bytestransfer
			if count >= target {
				break
			}
			res.Conn.AsynRecv('r', res.Buff, -1)
		} else {
			res.Conn.AsynSend('w', res.Buff, -1)
		}

	}
}
