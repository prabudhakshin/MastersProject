#! /usr/bin/env python

import sys
import optparse
import StringIO
import registered_domain as regdom
from collections import defaultdict as defdic

domainFileName = ""
dateRange = ""
queryTypes = ""
TOTBUCKETS = 200

def printHelpExit(parser, errormsg):
  print "%s\n" % errormsg
  print parser.print_help()
  sys.exit()

def get_regex(domain_list):
  data = {}

  for line in domain_list:
    tokens = line.split('.')
    if len(tokens) < 2:
      print 'WARNING: %s' % line
      continue
    tokens.reverse()
    if not tokens[0] in data:
      data[tokens[0]] = []
    buf = StringIO.StringIO()
    for t in tokens[1:]:
      buf.write('%s\\\\.' % t)
    data[tokens[0]].append(buf.getvalue()[:-3])

  buf = StringIO.StringIO()
  for key, val in data.items():
    buf2 = StringIO.StringIO()
    for v in val:
      buf2.write('%s|' % v)
    buf.write('(?:%s\\\\.(?:%s))|' % (key, buf2.getvalue()[:-1]))

  print '^%s$' % buf.getvalue()[:-1]
  return '^%s$' % buf.getvalue()[:-1]

def write_pig(inputstring, outputstring, qtypestring, regexstring):
    template_file = 'pig/template_raw.pig'
    buf = StringIO.StringIO()
    fin = open(template_file, 'r')
    for line in fin:
        buf.write(line)

    fin.close()
    template = buf.getvalue().strip()
    print template

    print template % {'input' : inputstring,
                      'qtypes': qtypestring,
                      'regex' : regexstring,
                      'output': outputstring}

def parseDateField(parser, dateRange):
  datePeriods = dateRange.split(",")
  days = []
  for period in datePeriods:
    rangevalue = period.split("-")
    if len(rangevalue) == 1:
      days.append(rangevalue[0])
    elif len(rangevalue) == 2:
      days = days + map (str, range(int(rangevalue[0]), int(rangevalue[1])+1))
    else:
      printHelpExit(parser, "Date period has more than one hiphen (-)")

  yearmonthToDayMap= defdic(list)
  for aday in days:
    yearmonth = aday[0:6]
    yearmonthToDayMap[yearmonth].append(aday)

  return yearmonthToDayMap

def parseArgs(argslist):
  usagestring = "Usage: %prog [options] domainlistfile"
  optparser = optparse.OptionParser(usage=usagestring)
  optparser.add_option("-p", "--period", dest="daterange", help="Range of days of the form '20120201,20120203,...' or 20120201-20120227 or 20120201-20120227,20120301-20120330", type=str, default="20120405");
  optparser.add_option("-q", "--querytype", dest="querytype", help="DNS querytypes (A, PTR, AAAA, OTHR) to search. * to include all query types", type=str, default="1");
  optparser.add_option("-o", "--outputfile", dest="outputfile", help="HDFS output dir path", type=str, default="output_raw");

  (options, args) = optparser.parse_args(argslist)

  if (len(args) != 1):
    printHelpExit(optparser, 'Invalid number of positional arguments.')

  domainFileName = args[0]
  dateRange = options.daterange
  yearmonthToDayMap = parseDateField(optparser, dateRange)
  qtypefield = options.querytype
  outputfile = "/user/pdhakshi/" + options.outputfile

  queryTypes = []
  if qtypefield == "*":
    queryTypes = ["0"]
  else:
    queryTypes = qtypefield.split(",")

  return domainFileName, yearmonthToDayMap, queryTypes, outputfile

def makeInputString(yearmonthToDayMap):
 # each input file is of the form:
 # /feeds/sie/ch202/201202/raw_processed.20120206*.0.gz
 basePath = '/feeds/sie/ch202/'
 fileNamePartial = '/raw_processed.{'
 fileExtension = '}*.0.gz'

 filelist = []
 for (yearmonth, daylist) in yearmonthToDayMap.items():
   namePartial = basePath + yearmonth + fileNamePartial
   filelist.append(namePartial + ",".join(daylist) + fileExtension)

 print ",".join(filelist)
 return ",".join(filelist)

def makequerystring(queryTypes):
  if len(queryTypes) == 1 and queryTypes[0]== "0":
    #wildcard case; Need to include all query types
    querystring = "qtype != 0"
    return querystring
  else:
    temp = []
    for atype in queryTypes:
      temp.append("(qtype == " + atype + ")")
    return " OR ".join(temp)

def main():

  domainFileName, yearmonthToDayMap, queryTypes, outputfilepath = parseArgs(sys.argv[1:])
  domainFileDesc = open(domainFileName, "r")
  domainList = []
  regDomainList = []

  # read domains form the input file
  for domain in domainFileDesc:
    domain = domain.strip().lower()
    if domain[-1] == '.':
      domain = domain[:-1]
    regdomain = regdom.get_registered_domain(domain)
    if not regdomain is None:
      domainList.append(domain)
      regDomainList.append(regdomain)
    else:
      print 'Domain %s does not have valid registered domain, skipping.' % domain

  inputstring = makeInputString(yearmonthToDayMap)
  querystring = makequerystring (queryTypes)
  regexstring = get_regex(regDomainList)
  write_pig(inputstring, outputfilepath, querystring, regexstring)

if __name__ == "__main__":
  main()
