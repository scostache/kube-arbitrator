#!/bin/sh
  
file=$1
contr=$2

# we get the delay of the job from Delay:

# we get the allocated : timestamp Allocated Real

mkdir ${file}-dir

grep Allocated $file | tr -s ' ' | cut -d ' ' -f 1,6,8 > ${file}-dir/alloc.cvs

grep PendingQueue $file | tr -s ' ' | cut -d ' ' -f 5,6 > ${file}-dir/queues.cvs
sed -i -e 's/=/ /g' ${file}-dir/queues.cvs ; cat ${file}-dir/queues.cvs | cut -d ' ' -f 2,4 > ${file}-dir/queue_numbers.cvs

grep DeclaredCompletion $file | cut -d '-' -f 2,3,4 > ${file}-dir/stats.txt
cat ${file}-dir/stats.txt | sort -u -n -k1 > ${file}-dir/stats.sorted.txt
cat ${file}-dir/stats.txt | cut -d ' ' -f 2,4 | cut -d '-' -f 2 | sort -u -n -k1 > ${file}-dir/delay.cvs

grep METRICS $contr | tr -s ' ' > $contr.tmp
grep Adding $contr.tmp | cut -d ' ' -f 6,11 > $contr.tmp.add
grep Starting $contr.tmp | tr -s ' ' | cut -d ' ' -f 6,9 > $contr.tmp.start

sort -n -u -k2 -t '-' $contr.tmp.add > ${file}-dir/controller.add.sorted.txt
sort -n -u -k2 -t '-' $contr.tmp.start > ${file}-dir/controller.start.sorted.txt

tar czf ${file}-dir.tgz ${file}-dir/
