#!/bin/bash
for i in {1..50}
do
   # go test -v ./kv/test_raftstore/ -run TestSplitConfChangeSnapshotUnreliableRecover3B  > output.log || exit
   make project3b >> output.log || exit
done