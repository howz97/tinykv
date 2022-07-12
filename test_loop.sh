#!/bin/bash
for i in {1..50}
do
   SECONDS=0
   make project_hard || exit
   echo "----- This test takes $SECONDS seconds to pass -----"
done

