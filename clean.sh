#!/bin/bash

rm -rf /tmp/flink*
rm -rf /tmp/hadoop*
rm -rf /tmp/rocksdb*
sleep 1
sudo sh -c "sync; echo 3 > /proc/sys/vm/drop_caches "
