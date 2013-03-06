REGISTER hdfs:///user/hadoop/jars/scala-library-2.9.1.jar;
REGISTER hdfs:///user/hadoop/jars/elephant-bird-2.0.4-SNAPSHOT.jar;
REGISTER hdfs:///user/hadoop/jars/google-collect-1.0.jar;
REGISTER hdfs:///user/hadoop/jars/json-simple-1.1.jar;
REGISTER hdfs:///user/hadoop/jars/commons-lang-2.6.jar;
REGISTER hdfs:///user/hadoop/jars/dnsparser_2.9.1-1.0.jar;
REGISTER hdfs:///user/apitsill/jars/apitsill.jar;
REGISTER hdfs:///user/hadoop/jars/dnsudf_2.9.1-1.0.jar;
REGISTER hdfs:///user/hadoop/jars/hadoopdns.jar;

DEFINE ExtractRegisteredDomain org.chris.dnsproc.ExtractRegisteredDomain();   
DEFINE ReverseDomain com.apitsill.pig.ReverseDomain();

-- Load files (this loads all files in the folder).
-- Specifying a folder recursively loads all files in it.
rawcsv = LOAD '%(input)s' as (line:chararray);

-- Parse the CSVs using Chris's parser
dnsrequests = FOREACH rawcsv
    GENERATE FLATTEN(
        org.chris.dnsproc.ParseDNSFast(line)
    ) as (
  ts: int,
  type: int,
  src_ip: chararray,
  dst_ip: chararray,
  domain: chararray,
  qtype: int,
  rcode: int,
  answer: bag { t: tuple(a:chararray, b:chararray, c:chararray, d:chararray, e:chararray) },
  authoritative: bag { t: tuple(a:chararray, b:chararray, c:chararray, d:chararray, e:chararray) },
  additional: bag { t: tuple(a:chararray, b:chararray, c:chararray, d:chararray, e:chararray) }
    );

-- Get all valid A-lookup records
p1 = FILTER dnsrequests by (ts is not null) AND (type == 0) AND (domain is not null) AND (%(qtypes)s);

-- Collect required columns, plus the reverse domain name
p2 = FOREACH p1 GENERATE ts, src_ip, domain, ExtractRegisteredDomain(LOWER(domain)) AS r_domain, qtype, answer;

p3 = FILTER p2 BY (r_domain is not null);

p4 = FOREACH p3 GENERATE ts, src_ip, domain, ReverseDomain(r_domain) AS rev_domain, qtype, answer;

-- Filter on domain name using Java-style regex syntax
p5 = FILTER p4 BY rev_domain MATCHES '%(regex)s';

-- Store results to disk
STORE p5 INTO '%(output)s';
