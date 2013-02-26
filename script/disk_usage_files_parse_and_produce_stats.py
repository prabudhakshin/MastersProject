#! /usr/bin/env python
import sys
import re
import numpy

fileName = sys.argv[1]
fileHandle = open(fileName, 'r')

fileSizeMBytes = []

for line in fileHandle:
  line = line.strip()
  filesizeCol, fileName = re.split(' *', line)
  fileSize = float(filesizeCol[:-1])
  if (filesizeCol[-1] == 'g'):
    fileSize = fileSize * 1024
  elif (filesizeCol[-1] == 'm'):
    fileSize = fileSize
  elif (filesizeCol[-1] == 'k'):
    fileSize = fileSize / 1024.0
  else:
    print ("Weird file size: {}",format(filesizeCol))
    pass
  fileSizeMBytes.append(fileSize)

print ("{:>20} | {:>6.2f}".format("Total Entries:", len(fileSizeMBytes)))
print ("{:>20} | {:>6.2f}".format("TotalSize in MB:", sum(fileSizeMBytes)))
print ("{:>20} | {:>6.2f}".format("Stand. Dev:", numpy.std(fileSizeMBytes)))
print ("{:>20} | {:>6.2f}".format("Mean:", numpy.mean(fileSizeMBytes)))
