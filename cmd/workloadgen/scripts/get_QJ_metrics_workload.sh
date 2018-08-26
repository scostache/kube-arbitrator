#!/bin/sh
  
file=$1

# we get the delay of the job from Delay:

# we get the allocated : timestamp Allocated Real

mkdir ${file}-dir

grep Allocated $file | tr -s ' ' | cut -d ' ' -f 1,6,8 > ${file}-dir/alloc.cvs

grep PendingQueue $file | tr -s ' ' | cut -d ' ' -f 5,6 > ${file}-dir/queues.cvs
sed -i -e 's/=/ /g' ${file}-dir/queues.cvs ; cat ${file}-dir/queues.cvs | cut -d ' ' -f 2,4 > ${file}-dir/queue_numbers.cvs

grep DeclaredCompletion $file > ${file}-dir/stats.txt
cat ${file}-dir/stats.txt | cut -d ' ' -f 2,4 | cut -d '-' -f 2 | sort -u -n -k1 > ${file}-dir/delay.cvs

tar czf ${file}-dir.tgz ${file}-dir/
