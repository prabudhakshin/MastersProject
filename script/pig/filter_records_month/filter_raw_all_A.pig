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
rawcsv = LOAD '/feeds/sie/ch202/201203/raw_processed.{20120301,20120302}*.0.gz,/feeds/sie/ch202/201202/raw_processed.{20120201,20120202,20120203,20120204,20120205,20120206,20120207,20120208,20120209,20120210,20120211,20120212,20120213,20120214,20120215,20120216,20120217,20120218,20120219,20120220,20120221,20120222,20120223,20120225,20120226,20120227,20120228,20120229}*.0.gz' as (line:chararray);

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
p1 = FILTER dnsrequests by (ts is not null) AND (type == 0) AND (domain is not null) AND ((qtype == 1));

-- Collect required columns, plus the reverse domain name
p2 = FOREACH p1 GENERATE ts, src_ip, domain, ExtractRegisteredDomain(LOWER(domain)) AS r_domain, qtype, answer;

p3 = FILTER p2 BY (r_domain is not null);

p4 = FOREACH p3 GENERATE ts, src_ip, domain, ReverseDomain(r_domain) AS rev_domain, qtype, answer;

-- Filter on domain name using Java-style regex syntax
p5 = FILTER p4 BY rev_domain MATCHES '^(?:info\\.(?:lot-of-pills|forerectiledysfunctions))|(?:ru\\.(?:beatgold|pinelast|drugsgreg89t|buysoftware-shop-7|inkdiamond|dgfj|flymagicnum))|(?:cn\\.(?:xdvirdei|kokulxj|gozulifel|wiracxm))|(?:au\\.(?:org\\.prostatehealth))|(?:su\\.(?:l0za))|(?:cz\\.(?:bitcoin))|(?:biz\\.(?:cnpill|allpharmacy))|(?:in\\.(?:store-buy-software))|(?:org\\.(?:wikipedia|santisimosacramento|tedifoundation|meteodesecoles|99k|bnlmci|zapisi|rangamarnath|freewebpage|shandon|adntwohrds|cscoa|crcoertk|devoiretmemoire|mirce|adbnwotrds|zzl|multifusion|hinisder|propeciacanada|cephalexin500mg|rxdd|medkoo|buyfluoxetineonline|7-pharmacy|viagrajelly|mac-isa|rxdelivery|bestrx|quarium-stakany))|(?:net\\.(?:ganriki|empe3|palaceplaystars|ehoh|server-c58|brentovates|athost|fragolineses|webgames-777|uvoweb|goldgamemummy|vombustortes|casinoluxglobal|thcshiephoa|argonas|fenerbahce12|comgymn|bigheadhosting|cogradafiortes|grandwisata|gulyaka|bigheadhosting|bestlife-gamez|dayuh|drestocoleras|cafes|kalitelihosting|game777global|vivavegasgame|cionitarotes|santacecilia|coreinovels|butgums|snowlam|tke3|allwaysearch|wmgd|overnightcialis|ergapharm|save-on-drugs|onlinepharmacyltd|cheaponlinepills|professionalviagra|counter-strike|norxpills|strangled|deepbit|scorevidic|a7aneek|ozcoin))|(?:com\\.(?:amoxicillin-buy|yjoci|sinmeds|mega|irataaqedy|imooqi|medicinesi|freewebportal|surrounded|ajaimueydyne|uybepiperoz|uhogaejelurees|ozyly|okouvoyeruosuk|nirydyhixovox|yvafiesyhycuaat|cipafaxoabayzal|sounds|vetyx|disuse|premiumpharmacydiscounts|good-bye-acne|qiaujoolok|brightly|puwevixov|ywaram|wikofumysuby|amutoocalila|cialiskaufen|abipo|edeaq|pharmacyreviewer|rxmaury719|nykoco|unoxilytuwebe|medicdl|nevamoreunion|ohubyeguoqof|owivefiuucavuv|aacidiac|myidrugstore|urunymiaha|pharmacy130|drugstorere|astartamed|canadianpharmacyzone|tton|ebagaloyly|bestofmedicines|hyysuzixoo|pehxr|medicineoftennessee|autugya|officialedmeds|edrugstorehelp|codmedicines|searchedmeds|kibgz|yuufih|idyiwaluvub|agymynyhoiquixu|yourppcworld|medicrgum|pyrereohe|ezovaburugoju|councilors|accords|qyhozayopy|easakyhalulip|dissmedic|usuafifyfejyfez|rx-pharmacy-links|yqafaepu|fats|drugssc|osayguwu|tininyeo|appropriateness|roewi|uzojyza|strabotours|seuqaatamaqit|atify|o4510|1medicaments|extradovolena|chmedecines|oyosv|aokr|resulting|medicellatu|cozeayveqic|medicrb|guests|pharmacy12|yfopuraqis|cheap-edpharma|fiendemed|ikihapyduori|walking))|(?:se\\.(?:mcss))$';

-- Store results to disk
STORE p5 INTO '/user/pdhakshi/pigouts/filter_results_month/filter_raw_all_A.gz';
