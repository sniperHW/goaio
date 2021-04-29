/*
 * single wather,single complete routine
 */

package main

import (
	"fmt"
	"github.com/sniperHW/goaio"
	"net"
	"time"
)

var clientcount int32
var bytescount int32
var packetcount int32

func main() {

	tcpAddr, err := net.ResolveTCPAddr("tcp", "localhost:8110")
	if err != nil {
		panic(err.Error())
	}

	ln, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		panic(err.Error())
	}

	go func() {
		for {
			res, err := goaio.GetCompleteStatus()
			if nil != err {
				return
			} else if nil != res.Err {
				fmt.Println("go error", res.Err)
				res.Conn.Close(res.Err)
			} else if res.Context.(rune) == 'r' {
				fmt.Println("on recv")
				res.Conn.Send('w', res.Buffs[0][:res.Bytestransfer])
			} else {
				fmt.Println("on send")
				res.Conn.Recv('r', res.Buffs[0][:cap(res.Buffs[0])])
			}
		}
	}()

	fmt.Println("server start at localhost:8110")

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}

		fmt.Println("new client")

		c, _ := goaio.Bind(conn, goaio.AIOConnOption{})

		c.SetRecvTimeout(time.Second * 5)

		c.Recv('r', make([]byte, 1024*4))

	}

}
