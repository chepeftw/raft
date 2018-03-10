#!/bin/bash

set -e

SCRIPT_HOME="$( cd "$( dirname "$0" )" && pwd )"
cd $SCRIPT_HOME

case "$1" in
  build)
      docker build -t raft-test .
    ;;
  start)
      docker run --privileged -dit --env RAFT_PORT=10123 --env RAFT_TIMEOUT=11000 --env RAFT_TARGET_SYNC=0 --name raft01 raft-test
    ;;
  stop)
      docker stop raft01
      docker rm raft01
    ;;
  log)
      docker exec raft01 tail -n 100 -f /var/log/golang/raft10123.log
    ;;
  *)
    echo $"Usage: $0 {up}"
    RETVAL=1
esac

cd - > /dev/null
