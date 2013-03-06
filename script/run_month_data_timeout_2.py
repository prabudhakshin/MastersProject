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
outputfile = "/home/pdhakshi/WorkSpace/data/process_month_data_2.log"

inputfilehandle = open(inputfile, "r")
outputfilehandle = open(outputfile, "a")

#yearmonth = sys.argv[2]
listofdays = [aday.strip() for aday in inputfilehandle]

TIMEOUT = 7200.0 # 2 hrs
for aday in listofdays:
  print >> outputfilehandle, "Handling day: %s-2" % (aday)
  yearmonth = aday[0:6]
  command = "pig -t ColumnMapKeyPrune -param paramday=%s.{[1][3-9],[2][0-3]} -param paramdayalone=%s -param paramyearmonth=%s -param dayhalf=2 multilevel_bucketing_splitday.pig" % (aday, aday, yearmonth)
  print 'Executing command: %s' % (command)

  returncode, returnstatus = RunCommand(command, TIMEOUT).Run()
  if returncode is not 0:
    print >> outputfilehandle, "Processing failed for day: %s-2; Reason: %s" % (aday, returnstatus)
  else:
    print >> outputfilehandle, "Processing successfully completed for day: %s-2" % (aday)
  outputfilehandle.flush()
