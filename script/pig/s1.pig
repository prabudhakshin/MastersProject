REGISTER /usr/lib/pig/piggybank.jar;
REGISTER myudfs.jar;
DEFINE Len org.apache.pig.piggybank.evaluation.string.LENGTH();

rawdata = load '../data/201212*' using PigStorage(' ', '-tagsource') as (filename:chararray, ts:int, ip:chararray, domain:chararray, answer:chararray);

temp = foreach rawdata generate ts, ip, domain, answer, myudfs.BucketizeByHashCode(domain) as domain_hashcode, filename, myudfs.ExtractTLD(domain) as tld;
dump temp;
-- A = foreach temp generate ts, ip, domain, answer, CONCAT(CONCAT(filename, '_'), domain_hashcode) as domain_index, filename;
-- B = foreach A generate ip as ip, domain_index, filename;
-- C = group B by (filename, ip);
-- D = foreach C generate FLATTEN(myudfs.MyUDF(group, B)) as (filename:chararray, ip:chararray, domain_buckets:chararray);
-- store D into '../data/output' using org.apache.pig.piggybank.storage.MultiStorage('../data/output', '0');
