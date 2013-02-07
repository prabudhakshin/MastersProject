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

-- Load files (this loads all files in the folder).
-- Specifying a folder recursively loads all files in it.
rawcsv = LOAD '/feeds/sie/ch202/201202/raw_processed.{20120201}*.0.gz' as (line:chararray);

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

p1 = FILTER dnsrequests by (ts is not null) AND (type == 0) AND (domain is not null);

-- Collect required columns, plus the reverse domain name
p2 = FOREACH p1 GENERATE ExtractRegisteredDomain(LOWER(domain)) AS r_domain;
-- p2 = FOREACH p1 GENERATE LOWER(domain) AS r_domain;

p3 = FILTER p2 by (r_domain is not null);

p4 = DISTINCT p3;

p5 = GROUP p4 ALL;

p6 = FOREACH p5 GENERATE FLATTEN($1);

STORE p6 INTO '/user/pdhakshi/output_regdomains';
