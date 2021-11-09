TESTDIR=${TESTDIR:="testdata/b100-n3"}
TYPE=${TYPE:="debug"}
W=${W:="80000"}

./target/$TYPE/node-net \
    --config $TESTDIR/nodes-$1.json \
    --ip ip_file \
    --sleep 20 \

