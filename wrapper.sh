#!/usr/bin/env sh

ETH0=$(ip a | grep eth0 | wc -l) # This is for ALPINE

while [ $ETH0 -eq 0 ]
do
  echo "waiting ... "
  sleep 2
  ETH0=$(ip a | grep eth0 | wc -l)
done

mkdir -p /var/log/golang
echo "START" > /var/log/golang/wrapper.log

echo "starting ... "
echo "---------------------------------------------"


# Start the first process
echo "Starting RAFT ... " >> /var/log/golang/wrapper.log
echo "/raft &" >> /var/log/golang/wrapper.log

/raft &
status=$?
if [ $status -ne 0 ]; then
  echo "Failed to start router process: $status"
  exit $status
fi

sleep 2

while /bin/true; do

  ps aux | grep raft | grep -v grep
  P1_STATUS=$?

  echo "PROCESS1 STATUS = $P1_STATUS |"

  echo "PROCESS1 STATUS = $P1_STATUS " >> /var/log/golang/wrapper.log

  # If the greps above find anything, they will exit with 0 status
  # If they are not both 0, then something is wrong
  if [ $P1_STATUS -ne 0 ]; then
    echo "RAFT has already exited."
    exit -1
  fi

  sleep 60
done