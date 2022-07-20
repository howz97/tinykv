#!/bin/bash
for i in {1..50}
do
   SECONDS=0
   make project3b || exit
   echo "----- The $i th test takes $SECONDS seconds to pass -----"
done
echo "Test loop finished."
