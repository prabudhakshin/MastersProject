#! /usr/bin/env python3

# Given input as "key #occurences", find the distribution of the keys
import sys
import re 

fileName = sys.argv[1].strip()
fileHandle = open(fileName, "r")

lines = []
for line in fileHandle:
  try:
    key,val = re.split("\s*", line.strip())
  except ValueError:
    print ('More than one value to unpack'),
    print (re.split("\s*", line.strip()))
    pass

  lines.append((key, int(val)))

lines.sort(key=lambda tup: tup[1], reverse=True)
total = 0
for line in lines:
  total = total + line[1]

# header = "{:>13} | {:<13} | {}".format("Query_Type","#Occurences", "% of total records")
# print (header)
# print ("-"*len(header))

if (len(sys.argv) > 2):
  lines = lines[:int(sys.argv[2])]
for line in lines:
  print ("{:>13} | {:<13} | {:<3.3f}".format(line[0], line[1], line[1]*100.0/total))
