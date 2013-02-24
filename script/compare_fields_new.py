#! /usr/bin/env python

import numpy as np
import matplotlib.pylab as plt
import sys

filehandle = open(sys.argv[1], "r")
header = filehandle.readline().strip()

fieldnames = [afield.strip() for afield in header.split("|")[1:]]
print fieldnames

# list of lists. Each sublist contains the values of different fields
fieldvaluelist = []
experimentnames = []
#start reading the values for each experiment
for line in filehandle:
  values = line.strip().split("|")
  experimentnames.append(values[0].strip())
  fieldvaluelist.append(np.array([int(avalue.strip()) for avalue in values[1:]]))

numfields = len(fieldnames)
xpos = np.arange(numfields)
barwidth = 1.0/(len(experimentnames)+1)
bars = []
colors = ['r', 'g', 'b', 'y', 'k', 'm']

baseline = fieldvaluelist[0]
for index, experiment in enumerate(fieldvaluelist):
  values = experiment.astype(float)/baseline.astype(float)
  curxpos = xpos + barwidth*index
  abar = plt.bar(xpos+barwidth*index, values, width=barwidth, color=colors[index])
  bars.append(abar)
  vv = map(None, values)
  for i, avalue in enumerate(vv):
    plt.text(curxpos[i]+barwidth/2.0, values[i]+0.05, "%0.2f" % (float(avalue)*100.0) + "%", ha='center', va='bottom', rotation=90)

plt.legend(bars, experimentnames)
xtickpos = xpos + 0.5
plt.xticks(xtickpos, fieldnames)
#plt.ylabel("Relative values", fontsize=20)
plt.show()
