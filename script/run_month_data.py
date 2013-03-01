#! /usr/bin/env python
import sys 
import shlex
import subprocess
from subprocess import CalledProcessError

def runcommand(command):
  args = shlex.split(command)
  try:
    subprocess.check_call(args)
    return 0
  except CalledProcessError as e:
    return e.returncode

inputfile = sys.argv[1]
outputfile = "/home/pdhakshi/WorkSpace/data/process_month_data.log"

inputfilehandle = open(inputfile, "r")
outputfilehandle = open(outputfile, "w")

#yearmonth = sys.argv[2]
listofdays = [aday.strip() for aday in inputfilehandle]

for aday in listofdays:
  print >> outputfilehandle, "Handling day: %s" % (aday)
  yearmonth = aday[0:6]
  command = "pig -t ColumnMapKeyPrune -param paramday=%s -param paramyearmonth=%s multilevel_bucketing.pig" % (aday, yearmonth)
  #command = "pig -param paramday=%s -param paramyearmonth=%s dedup_reformat.pig" % (aday, yearmonth)
  #command = "pig -param paramday=%s -param paramyearmonth=%s just_bucketize.pig" % (aday, yearmonth)
  #command = "pig -param paramday=%s raw_change_compression.pig" % (aday)
  #command = "pig -param paramday=%s dedup_reformat.pig" % (aday)
  print 'Executing command: %s' % (command)

  if runcommand(command) is not 0:
    print "Processing failed for day: %s" % (aday)
    print >> outputfilehandle, "Processing failed for day: %s" % (aday)
  else:
    print >> outputfilehandle, "Processing successfully completed for day: %s" % (aday)
  outputfilehandle.flush()
