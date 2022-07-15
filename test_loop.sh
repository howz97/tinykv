#!/bin/bash
for i in {1..10}
do
   SECONDS=0
   make project_hard || exit
   echo "----- The $i th test takes $SECONDS seconds to pass -----"
done
echo "Test loop finished."
