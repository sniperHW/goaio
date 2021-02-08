/*
 * single wather,single complete routine
 */

package main

import (
	"fmt"
	"github.com/sniperHW/goaio"
	"net"
	"net/http"
	_ "net/http/pprof"
	"time"
)

var clientcount int32
var bytescount int32
var packetcount int32

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
			err, conn, buff, bytestransfer, context := service.GetCompleteStatus()
			if nil != err {
				if err == ErrServiceClosed {
					return
				}
				fmt.Println(err)
				conn.Close()
			} else {
				if context.(rune) == 'r' {
					conn.Send(buff[:bytestransfer], 'w')
				} else {
					conn.Recv(buff[:cap(buff)], 'r')
				}
			}
		}
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}

		c := service.Bind(conn)

		c.SetRecvTimeout(time.Second * 5)

		buff := make([]byte, 1024*4)

		c.Recv(buff, 'r')

	}

}
