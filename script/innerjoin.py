#! /usr/bin/env python3

import sys
import re
import matplotlib.pyplot as plot

file1 = sys.argv[1].strip()
file2 = sys.argv[2].strip()

file1Handle = open(file1, 'r')
file2Handle = open(file2, 'r')

file1Contents = []
file2Contents = []

for line in file1Handle:
  (ip, count) = line.strip().split('|');
  ip = re.sub(r'\s','',ip)  
  count = re.sub(r'\s','',count)
  file1Contents.append((ip,int(count)))

for line in file2Handle:
  (ip, count) = line.strip().split('|');
  ip = re.sub(r'\s','',ip)  
  count = re.sub(r'\s','',count)
  file2Contents.append((ip,int(count)))

plot1  = []
plot2  = []

for item1 in file1Contents:
  for item2 in file2Contents:
    if item1[0] == item2[0]:
      plot1.append('{:.3f}'.format((item1[1]/26.0)*100))
      plot2.append('{:.3f}'.format((item2[1]/676.0)*100))
      # print ('{:>17} | {:>3} {:>7.3f} % | {:>3} {:>7.3f} %'.format(item1[0], item1[1], (item1[1]/26.0)*100, item2[1], (item2[1]/676.0)*100))

t = arange(0, len(plot1), 1)
plot(t, plot1, t, plot2)
show()
