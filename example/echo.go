/*
 * single wather,single complete routine
 */

package main

import (
	"fmt"
	"github.com/sniperHW/goaio"
	//"github.com/sniperHW/kendynet/timer"
	"net"
	"net/http"
	_ "net/http/pprof"
	//"sync/atomic"
	//"time"
)

var clientcount int32
var bytescount int32
var packetcount int32

/*type sendContext struct {
	buff []byte
}

type recvContext struct {
	buff []byte
}*/

type recvbuff []byte
type sendbuff []byte

func main() {

	go func() {
		http.ListenAndServe("0.0.0.0:6060", nil)
	}()

	tcpAddr, err := net.ResolveTCPAddr("tcp", "localhost:8110")
	if err != nil {
		panic(err.Error())
	}

	ln, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		panic(err.Error())
	}

	service := goaio.NewAIOService(4)

	go func() {
		for {
			err, conn, bytestransfer, context := service.GetCompleteStatus()
			if nil != err {
				fmt.Println("close")
				conn.Close()
			} else {
				switch context.(type) {
				case recvbuff:
					//fmt.Println("send----")
					conn.Send([]byte(context.(recvbuff))[:bytestransfer], sendbuff(context.(recvbuff)))
				case sendbuff:
					//fmt.Println("recv----")
					conn.Recv([]byte(context.(sendbuff)), recvbuff(context.(sendbuff)))
				}

			}
		}
	}()

	/*timer.Repeat(time.Second, func(_ *timer.Timer, ctx interface{}) {
		tmp1 := atomic.LoadInt32(&bytescount)
		tmp2 := atomic.LoadInt32(&packetcount)
		atomic.StoreInt32(&bytescount, 0)
		atomic.StoreInt32(&packetcount, 0)
		fmt.Printf("clientcount:%d,transrfer:%d KB/s,packetcount:%d\n", atomic.LoadInt32(&clientcount), tmp1/1024, tmp2)
	}, nil)*/

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}

		fmt.Println("onclient")

		c := service.Bind(conn)

		buff := make([]byte, 1024*4)

		c.Recv(buff, recvbuff(buff))

	}

}
