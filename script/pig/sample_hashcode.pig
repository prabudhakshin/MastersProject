REGISTER hdfs:///user/pdhakshi/jars/piggybank.jar;
REGISTER hdfs:///user/pdhakshi/jars/myudfs.jar;
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
-- rawcsv = LOAD '/feeds/sie/ch202/201202/raw_processed.{20120201,20120202,20120203}*.0.gz' using PigStorage('\t','-tagsource') as (inputpath:chararray, line:chararray);
rawcsv = LOAD '/feeds/sie/ch202/201202/raw_processed.{20120201}*.0.gz' using PigStorage('\t','-tagsource') as (inputpath:chararray, line:chararray);

-- Parse the CSVs using Chris's parser
dnsrequests = FOREACH rawcsv
    GENERATE inputpath as inputpath, FLATTEN(org.chris.dnsproc.ParseDNSFast(line)) as (
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
p1 = FILTER dnsrequests by (ts is not null) AND (type == 0) AND (qtype == 1) AND (domain is not null);

-- Collect required columns, plus the reverse domain name
p2 = FOREACH p1 GENERATE ts, src_ip, domain, ExtractRegisteredDomain(LOWER(domain)) AS r_domain, qtype, answer, authoritative, additional, myudfs.ParseDayFromFileName(inputpath) as filename;
-- 
p3 = FILTER p2 BY (r_domain is not null);

p4 = FOREACH p3 GENERATE ts, src_ip, domain, ReverseDomain(r_domain) AS rev_domain, qtype, answer, authoritative, additional, UPPER(SUBSTRING(r_domain, 0, 2)) as domain_first_char, myudfs.BucketizeByHashCode(r_domain) as domain_hashcode:chararray, filename;
-- p4 = FOREACH p3 GENERATE ts, src_ip, domain, ReverseDomain(r_domain) AS rev_domain, qtype, answer, authoritative, additional, UPPER(SUBSTRING(r_domain, 0, 2)) as domain_first_char, filename;

p5 = FILTER p4 by domain_first_char MATCHES '[A-Z][A-Z]';

p6 = FOREACH p5 GENERATE ts, src_ip, domain, rev_domain, qtype, answer, authoritative, additional, CONCAT(CONCAT(filename, '_'), domain_hashcode) as domain_bucket, filename;
-- p6 = FOREACH p5 GENERATE ts, src_ip, domain, rev_domain, qtype, answer, authoritative, additional, CONCAT(CONCAT(filename, '_'), domain_first_char) as domain_bucket, filename;

p7 = FOREACH p6 GENERATE src_ip, domain_bucket, filename;

p8 = GROUP p7 by (filename, src_ip);

p9 = FOREACH p8 generate FLATTEN(myudfs.MyUDF(group, p7)) as (filename:chararray, src_ip:chararray, domain_buckets:chararray);

STORE p6 INTO '/user/pdhakshi/SIE_DATA/BY_DOMAIN_hashbucketing_300/' using org.apache.pig.piggybank.storage.MultiStorage('/user/pdhakshi/SIE_DATA/BY_DOMAIN_hashbucketing_300/', '8', 'gz');
-- STORE p6 INTO '/user/pdhakshi/SIE_DATA/BY_DOMAIN_hashbucketing/';

STORE p9 INTO '/user/pdhakshi/SIE_DATA/BY_RESOLVER_hashbucketing_300/' using org.apache.pig.piggybank.storage.MultiStorage('/user/pdhakshi/SIE_DATA/BY_RESOLVER_hashbucketing_300/', '0', 'gz');
-- STORE p9 INTO '/user/pdhakshi/SIE_DATA/BY_RESOLVER_hashbucketing/';
