#! /usr/bin/env python3

# BY_RESOLVER data contains the resolver ip address and the list of 
# domain buckets where the resolver data is found.
# This script finds the #domain_buckets for each ip_address and lists
#              ip_address    #occurences
# in decreasing order of #occurences.

import sys
import re

def tuple_cmp(x):
  return x[1]

fileName = sys.argv[1].strip()
fileHandle = open(fileName, 'r')
result = []
for record in fileHandle:
  record = record.strip()
  (ts, ip, domain_buckets) = re.split("\t", record)
  result.append((ip, len(domain_buckets.split(" "))))

result = sorted(result, key=tuple_cmp, reverse=True)

for record in result:
  print ('{:>17} | {}'.format(record[0], record[1]))
