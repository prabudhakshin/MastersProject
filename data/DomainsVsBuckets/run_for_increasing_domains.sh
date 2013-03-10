#! /bin/sh

nums=`seq 1 1000`;
for i in $nums
do
  #head --lines=$i ../GITREPO/data/1000_mixed_samples.txt > doms.txt;
  head --lines=$i ../GITREPO/data/1000_com_samples.txt > doms.txt;
  #../GITREPO/script/FindBuckets.py -q "*" doms.txt -p 20120202;
  ../GITREPO/script/FindBuckets.py doms.txt -p 20120202;
done
