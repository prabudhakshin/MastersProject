#! /usr/bin/env python

import sys
import optparse
import StringIO
import registered_domain as regdom
from collections import defaultdict as defdic
import re

domainFileName = ""
dateRange = ""
queryTypes = ""
TOTBUCKETS = 200

qtypeCodeToNameMap = {"1"  : "A",
                      "12" : "PTR",
                      "28" : "AAAA"}
qtypeNameToCodeMap = {      "KX" : "36",
                           "KEY" : "25",
                         "DHCID" : "49",
                            "DS" : "43",
                        "DNSKEY" : "48",
                          "CERT" : "37",
                           "APL" : "42",
                           "SOA" : "6",
                           "LOC" : "29",
                           "SPF" : "99",
                           "SIG" : "24",
                         "RRSIG" : "46",
                             "A" : "1",
                          "AAAA" : "28",
                         "SSHFP" : "44",
                           "PTR" : "12",
                          "NSEC" : "47",
                            "TA" : "32768",
                      "NSEC3PARAM" : "51",
                           "TXT" : "16",
                           "HIP" : "55",
                            "MX" : "15",
                         "AFSDB" : "18",
                      "IPSECKEY" : "45",
                            "RP" : "17",
                           "SRV" : "33",
                           "DLV" : "32769",
                         "DNAME" : "39",
                            "NS" : "2",
                          "TSIG" : "250",
                           "CAA" : "257",
                         "NSEC3" : "50",
                         "NAPTR" : "35",
                         "CNAME" : "5",
                          "TLSA" : "52",
                          "TKEY" : "249"}

acceptedQueryTypeCodes = ["36","25","49","43","48","37","42","6","29","99",
                          "24","46","1","28","44","12","47","32768","51","16",
                          "55","15","18","45","17","33","32769","39","2","250",
                          "257","50","35","5","52","249"]

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
    template_file = 'pig/template_smart.pig'
    buf = StringIO.StringIO()
    fin = open(template_file, 'r')    
    for line in fin:
        buf.write(line)

    fin.close()
    template = buf.getvalue().strip()

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
  return days

def parseArgs(argslist):
  usagestring = "Usage: %prog [options] domainlistfile"
  optparser = optparse.OptionParser(usage=usagestring)
  optparser.add_option("-p", "--period", dest="daterange", help="Range of days of the form '20120201,20120203,...' or 20120201-20120227 or 20120201-20120227,20120301-20120330", type=str, default="20120405");
  optparser.add_option("-q", "--querytype", dest="querytype", help="Comma separated DNS querytypes (1,12,28...) to search. * to include all query types", type=str, default="1");

  (options, args) = optparser.parse_args(argslist)

  if (len(args) != 1):
    printHelpExit(optparser, 'Invalid number of positional arguments.')

  domainFileName = args[0]
  dateRange = options.daterange
  dateRange = parseDateField(optparser, dateRange)
  qtypefield = options.querytype

  queryTypeName = []
  queryTypeCode = []
  re_qcode = re.compile("^\d+$")
  re_qname = re.compile("^[a-zA-Z]+$")

  if qtypefield == "*":
    queryTypeName = ["A", "PTR", "AAAA", "OTHR"]
    queryTypeCode = ["0"]
  else:
    queryTypes = qtypefield.split(",")
    for atype in queryTypes:
      atype = atype.strip() 
      # Check if query type code was supplied
      if re_qcode.match(atype):
        if atype not in acceptedQueryTypeCodes:
          printHelpExit(optparser, 'Given query type "%s" is not valid.' % (atype))
        if atype in qtypeCodeToNameMap:
          if qtypeCodeToNameMap[atype] not in queryTypeName:
            queryTypeName.append(qtypeCodeToNameMap[atype])
        else:
          if "OTHR" not in queryTypeName:
            queryTypeName.append("OTHR")

        # add the query type code to the list
        if atype not in queryTypeCode:
          queryTypeCode.append(atype)

      # check if query type name was supplied
      elif re_qname.match(atype):
        if atype in qtypeNameToCodeMap:
          if qtypeNameToCodeMap[atype] not in queryTypeCode:
            queryTypeCode.append(qtypeNameToCodeMap[atype])
        else:
           printHelpExit(optparser, 'Given query type "%s" is not valid.' % (atype))

        if atype not in queryTypeName:
          queryTypeName.append(atype) 

      else:
         printHelpExit(optparser, 'Given query type "%s" is not valid.' % (atype))

  return domainFileName, dateRange, queryTypeName, queryTypeCode

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

def findFiles(domainList, queryTypes, dateRange):
  result = []
  for domain in domainList:
    regDomain = regdom.get_registered_domain(domain)

    if (regDomain[-1] == '.'):
      regDomain = regDomain[:-1]

    revRegDomain = ".".join(regDomain.split(".")[::-1])

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
      if aResult not in result:
        result.append(aResult)

  return result

def makeInputString(filesToSearch):
  daytofilesmap = defdic(list)
  for afile in filesToSearch:
    yearmonthday = afile[0:8]
    daytofilesmap[yearmonthday].append(afile)

  basepath = "/user/pdhakshi/SIE_DATA/BY_MULTIPARAMS/"
  outputfilelist = []
  for (aday, filelist) in daytofilesmap.items():
    outputfilelist.append("%s/{%s}.gz/*" % (aday, ",".join(filelist)))

  return basepath + "{" + ",".join(outputfilelist) + "}"


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

  domainFileName, dateRange, queryTypes, queryTypeCodes = parseArgs(sys.argv[1:])
  print queryTypes
  print queryTypeCodes
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

  filesToSearch = findFiles(domainList, queryTypes, dateRange)
  fileinputstring = makeInputString(filesToSearch)
#  fileinputstring = "/user/pdhakshi/SIE_DATA/BY_MULTIPARAMS/{%s}.gz/*" % (",".join(filesToSearch))
  regexstring = get_regex(regDomainList)

  for aFile in filesToSearch:
    print aFile

  querystring = makequerystring (queryTypeCodes)
  write_pig(fileinputstring, "/user/pdhakshi/output", querystring, regexstring)

if __name__ == "__main__":
  main()
