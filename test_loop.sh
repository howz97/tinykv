#!/bin/bash
for i in {1..10}
do
   go test -v ./kv/test_raftstore/ -run TestSplitConfChangeSnapshotUnreliableRecover3B  > output.log || exit
done