#!/bin/bash

set -e

echo ""
echo "--- BENCH ECHO START ---"
echo ""

#cd $(dirname "${BASH_SOURCE[0]}")
function cleanup {
    echo "--- BENCH ECHO DONE ---"
    kill -9 $(jobs -rp)
    wait $(jobs -rp) 2>/dev/null
}
trap cleanup EXIT

#mkdir -p bin
#$(pkill -9 net-echo-server || printf "")
#$(pkill -9 evio-echo-server || printf "")

function gobench {
    echo "--- $1 ---"
    if [ "$3" != "" ]; then
        go build -o $2 $3
    fi
    #GOMAXPROCS=1 $2 --port $4 &
    
    #GOMAXPROCS=1 $2 > out.txt &

    $2 > out.txt &

    sleep 1
    echo "*** 50 connections, 10 seconds, 6 byte packets"
    nl=$'\r\n'
    tcpkali --workers 1 -c 50 -T 10s -m "PING{$nl}" $4
    echo "--- DONE ---"
    echo ""
}

gobench "echo" ./echo  ./echo.go 127.0.0.1:8110
#gobench "echo_mutil_cpqueue" ./echo_mutil_cpqueue  ./echo_mutil_cpqueue.go 127.0.0.1:8111
#gobench "echo_mutil_watcher_cpqueue" ./echo_mutil_watcher_cpqueue  ./echo_mutil_watcher_cpqueue.go 127.0.0.1:8112
#gobench "echo_one_cpque_mutil_routine" ./echo_one_cpque_mutil_routine  ./echo_one_cpque_mutil_routine.go 127.0.0.1:8113
#gobench "EVIO" bin/evio-echo-server ../examples/echo-server/main.go 5002
