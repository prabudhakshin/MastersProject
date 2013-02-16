#! /usr/bin/env python

import sys
import optparse
import StringIO
import registered_domain as regdom

domainFileName = ""
dateRange = ""
queryTypes = ""
TOTBUCKETS = 200

bucket_distribution = {
  "A_COM": 34.418,
  "A_NET": 21.807,
  "A_ARPA": 0.129,
  "A_OTHR": 9.910,
  "PTR_COM": 0.108,
  "PTR_NET": 0.063,
  "PTR_ARPA": 14.869,
  "PTR_OTHR": 0.029,
  "AAAA_COM": 7.225,
  "AAAA_NET": 3.468,
  "AAAA_ARPA": 0.004,
  "AAAA_OTHR": 2.407,
  "OTHR_COM": 2.703,
  "OTHR_NET": 1.390,
  "OTHR_ARPA": 0.306,
  "OTHR_OTHR": 1.163 }

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
  return '^(%s)\\\\.$' % buf.getvalue()[:-1]

def write_pig(fileinputstring, regexstring, outputstring):
    template_file = 'pig/template_raw.pig'
    buf = StringIO.StringIO()
    fin = open(template_file, 'r')    
    for line in fin:
        buf.write(line)

    fin.close()
    template = buf.getvalue().strip()

    print template % {'input': fileinputstring,
                      'output': outputstring,
                      'regex': regexstring}

def parseArgs(argslist):
  global domainFileName, queryTypes, dateRange
  usagestring = "Usage: %prog [options] domainlistfile"
  optparser = optparse.OptionParser(usage=usagestring)
  optparser.add_option("-p", "--period", dest="daterange", help="Range of days of the form '20120201,20120203,...'", type=str, default="20120405");
  optparser.add_option("-q", "--querytype", dest="querytype", help="DNS querytypes to search for (like 'A,PTR,...')", type=str, default="A");

  (options, args) = optparser.parse_args(argslist)

  if (len(args) != 1):
    printHelpExit(optparser, 'Invalid number of positional arguments.')

  domainFileName = args[0]
  dateRange = options.daterange
  queryTypes = options.querytype

def getJavahash(s):
  h = 0
  for c in s:
    h = (31 * h + ord(c)) & 0xFFFFFFFF
  hashvalue = ((h + 0x80000000) & 0xFFFFFFFF) - 0x80000000
  return hashvalue & 0x00000000ffffffff;

def getBucketNumber(percent_dist_for_group, domainHashCode):
  global TOTBUCKETS
  bucketCount = int(TOTBUCKETS*percent_dist_for_group/100.0)
  if (bucketCount < 1):
    bucketCount = 1

  print 'numbuckets: ', bucketCount
  return str(domainHashCode % bucketCount)

def findFiles(domainList):
  result = set()
  for domain in domainList:
    regDomain = regdom.get_registered_domain(domain)

    if (regDomain[-1] == '.'):
      regDomain = regDomain[:-1]

    revRegDomain = regDomain[::-1]

    tld = revRegDomain.split('.')[0]
    tld = tld.upper()

    if (tld != "COM" and
        tld != "NET" and
        tld != "ARPA"):
        tld = "OTHR"

    domainHashCode = getJavahash(revRegDomain)
    print revRegDomain, domainHashCode

    qtype_tld_list = []
    for qtype in queryTypes:
      qtype_tld = qtype + "_" + tld
      percent_dist_for_group = bucket_distribution[qtype_tld]
      bucketnumber = getBucketNumber(percent_dist_for_group, domainHashCode)
      qtype_tld_list.append(qtype_tld + "_" + bucketnumber)

    temp_result = [aDay + "_" + qtype_tld for aDay in dateRange for qtype_tld in qtype_tld_list]

    for aResult in temp_result:
      result.add(aResult)

  return list(result)

def main():

  global domainFileName, queryTypes, dateRange
  parseArgs(sys.argv[1:])
  queryTypes = queryTypes.strip().split(",")
  dateRange = dateRange.strip().split(",")

  domainFileDesc = open(domainFileName, "r")
  domainList = []

  # read domains form the input file
  for domain in domainFileDesc:
    domain = domain.strip().lower()
    if domain[-1] == '.':
      domain = domain[:-1]
    regdomain = regdom.get_registered_domain(domain)
    if not regdomain is None:
      domainList.append(domain)
    else:
      print 'Domain %s does not have valid registered domain, skipping.' % domain

  filesToSearch = findFiles(domainList)
  fileinputstring = "/user/pdhakshi/SIE_DATA/BY_MULTIPARAMS_1day/{%s}.gz/*" % (",".join(filesToSearch))
  regexstring = get_regex(domainList)

  for aFile in filesToSearch:
    print aFile

  write_pig(fileinputstring, regexstring, "/user/pdhakshi/output")

if __name__ == "__main__":
  main()
