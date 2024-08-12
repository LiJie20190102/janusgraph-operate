#!/bin/bash


BIN_DIR=$(dirname $0)
BIN_DIR=$(
  cd "$BIN_DIR"
  pwd
)

source ${BIN_DIR}/service-env.sh

killForceFlag=$1

function stop()
{
    server_pid=$(get_server_pid)
    if [ -z "$server_pid" ]
    then
        echo "Maybe $SERVER_NAME not running, please check it..."
    else
        echo -n "The $SERVER_NAME is stopping..."
        if [ "$killForceFlag" == "-f" ]
        then
            echo "by force"
            kill -9 $server_pid
        else
            echo
            kill $server_pid
        fi
    fi
}

stop
