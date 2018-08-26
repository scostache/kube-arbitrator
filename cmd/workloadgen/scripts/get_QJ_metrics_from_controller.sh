#!/bin/sh
  
file=$1

grep METRICS $file | tr -s ' ' > $file.tmp

grep Adding $file.tmp | cut -d ' ' -f 6,11 > $file.tmp.add
grep Starting $file.tmp | tr -s ' ' | cut -d ' ' -f 6,9 > $file.tmp.start

sort -n -u -k2 -t '-' $file.tmp.add > $file.tmp.add.sorted.txt

sort -n -u -k2 -t '-' $file.tmp.start > $file.tmp.start.sorted.txt
