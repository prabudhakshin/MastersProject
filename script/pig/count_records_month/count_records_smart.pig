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
-- rawcsv = LOAD '/user/pdhakshi/pigouts/dedup_reformat/20120201.gz/*.gz' as (line:chararray);
rawcsv = LOAD '/user/pdhakshi/pigouts/smart_data_2months/{20120201,20120202,20120203,20120204,20120205,20120206,20120207,20120208,20120209,20120210,20120211,20120212,20120213,20120214,20120215,20120216,20120217,20120218,20120219,20120220,20120221,20120222,20120223,20120225,20120226,20120227,20120228,20120229,20120301,20120302}*/*.gz/*.gz' as (line:chararray);
p1 = foreach rawcsv generate 1 as one;
p2 = foreach (group p1 all) generate COUNT($1);
store p2 into '/user/pdhakshi/pigouts/record_count_month/smart';
