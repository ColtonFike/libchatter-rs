#!/bin/bash
# A script to test quickly

killall node-benchmark &> /dev/null

TESTDIR=${TESTDIR:="testdata/n100"}
W=${W:="80000"}

for i in {0..99}
do
  ./target/debug/node-benchmark \
      --config $TESTDIR/nodes-$i.json \
      --ip ip_file \
      --sleep 20 \
      -s $1 &> $i.log &
done

sleep 60

# Client has finished; Kill the nodes
killall ./target/debug/node-benchmark &> /dev/null
