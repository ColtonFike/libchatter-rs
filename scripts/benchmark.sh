#!/bin/bash
# A script to test quickly

killall node-benchmark &> /dev/null

TESTDIR=${TESTDIR:="testdata/n60"}
W=${W:="80000"}

for i in {0..59}
do
  ./target/release/node-benchmark \
      --config $TESTDIR/nodes-$i.json \
      --ip ip_file \
      --sleep 20 \
      -s $1 &> $i.log &
done

