#! /usr/bin/env python
import sys 
import shlex
import subprocess
import threading

class RunCommand(threading.Thread):
  def __init__(self, commandstring, timeoutinsecs):
    threading.Thread.__init__(self)
    self.timeout = timeoutinsecs
    self.command = shlex.split(commandstring)
    self.returnstatus = ""

  def run(self):
    # start the command in a new process and wait for its termination
    self.processHandle = subprocess.Popen(self.command)
    self.processHandle.wait()
    self.returncode = self.processHandle.returncode

  def Run(self):
    #start the thread
    self.start()
    # wait for atmost timeout secs for the thread to finish
    self.join(self.timeout)

    # check if the thread is still alive after timeout secs
    if self.is_alive():
      # kill the subprocess that the thread spawned. This will wake the thread
      # from the wait call and the thread will also eventually temrinate.
      self.processHandle.terminate()
      # Now after terminating the subprocess, wait for the thread to terminate.
      self.join()
      self.returnstatus = "Killed the process after timeout."

    return self.returncode, self.returnstatus

inputfile = sys.argv[1]
#outputfile = "/home/pdhakshi/WorkSpace/data/process_month_data.log"
outputfile = "/home/pdhakshi/WorkSpace/data/process_month_data_1.log"

inputfilehandle = open(inputfile, "r")
outputfilehandle = open(outputfile, "a")

#yearmonth = sys.argv[2]
listofdays = [aday.strip() for aday in inputfilehandle]

TIMEOUT = 72000.0 # 2 hrs
for aday in listofdays:
  print >> outputfilehandle, "Handling day: %s-1" % (aday)
  yearmonth = aday[0:6]
  command = "pig -t ColumnMapKeyPrune -param paramday=%s.{[0][0-9],[1][0-2]} -param paramdayalone=%s -param paramyearmonth=%s -param dayhalf=1 multilevel_bucketing_splitday.pig" % (aday, aday, yearmonth)
  #command = "pig -t ColumnMapKeyPrune -param paramday=%s -param paramyearmonth=%s multilevel_bucketing.pig" % (aday, yearmonth)
  #command = "pig -param paramday=%s -param paramyearmonth=%s dedup_reformat.pig" % (aday, yearmonth)
  #command = "pig -t ColumnMapKeyPrune -param paramday=%s -param paramyearmonth=%s just_bucketize.pig" % (aday, yearmonth)
  #command = "pig -param paramday=%s raw_change_compression.pig" % (aday)
  #command = "pig -param paramday=%s dedup_reformat.pig" % (aday)
  print 'Executing command: %s' % (command)

  returncode, returnstatus = RunCommand(command, TIMEOUT).Run()
  if returncode is not 0:
    print >> outputfilehandle, "Processing failed for day: %s-1; Reason: %s" % (aday, returnstatus)
  else:
    print >> outputfilehandle, "Processing successfully completed for day: %s-1" % (aday)
  outputfilehandle.flush()
