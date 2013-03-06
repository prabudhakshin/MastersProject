#! /bin/sh
echo "running filter_raw_com_A.pig" >> runscriptlog.txt
echo "clearing caches..."
../clear_cache.sh
pig filter_raw_com_A.pig

echo "running filter_bz2_com_A.pig" >> runscriptlog.txt
echo "clearing caches..."
../clear_cache.sh
pig filter_bz2_com_A.pig 

echo "running filter_just_bucketing_com_A.pig" >> runscriptlog.txt
echo "clearing caches..."
../clear_cache.sh
pig filter_just_bucketing_com_A.pig

echo "running filter_ref_ded_com_A.pig" >> runscriptlog.txt
echo "clearing caches..."
../clear_cache.sh
pig filter_ref_ded_com_A.pig

echo "running filter_smart_com_A.pig" >> runscriptlog.txt
echo "clearing caches..."
../clear_cache.sh
pig filter_smart_com_A.pig 

echo "running filter_raw_com_all.pig" >> runscriptlog.txt
echo "clearing caches..."
../clear_cache.sh
pig filter_raw_com_all.pig

echo "running filter_smart_com_all.pig" >> runscriptlog.txt
echo "clearing caches..."
../clear_cache.sh
pig filter_smart_com_all.pig

echo "running filter_raw_all_A.pig" >> runscriptlog.txt
echo "clearing caches..."
../clear_cache.sh
pig filter_raw_all_A.pig

echo "running filter_smart_all_A.pig" >> runscriptlog.txt
echo "clearing caches..."
../clear_cache.sh
pig filter_smart_all_A.pig

echo "running filter_raw_all_all.pig" >> runscriptlog.txt
echo "clearing caches..."
../clear_cache.sh
pig filter_raw_all_all.pig

echo "running filter_smart_all_all.pig" >> runscriptlog.txt
echo "clearing caches..."
../clear_cache.sh
pig filter_smart_all_all.pig
