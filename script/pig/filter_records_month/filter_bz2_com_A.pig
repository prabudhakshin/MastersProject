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
rawcsv = LOAD '/user/pdhakshi/pigouts/raw_bz2_month/{20120201,20120202,20120203,20120204,20120205,20120206,20120207,20120208,20120209,20120210,20120211,20120212,20120213,20120214,20120215,20120216,20120217,20120218,20120219,20120220,20120221,20120222,20120223,20120225,20120226,20120227,20120228,20120229,20120301,20120302}.bz2/*.bz2' as (line:chararray);

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
p5 = FILTER p4 BY rev_domain MATCHES '^(?:com\\.(?:easakyhalulip|uybepiperoz|icon|uxuayv|ayzoqeopely|uiafytyjokiy|wiujo|irataaqedy|yuufih|job|ybucataoo|atify|extradovolena|ynufyuz|walking|qiaujoolok|ywaluubyo|disuse|guests|appropriateness|ajopiicu|osayguwu|councilors|ozyly|surrounded|abejy|yjoci|imooqi|architect|lyemia|ezovaburugoju|peyidupyb|mepifiqovyyyv|cheap-edpharma|pehxr|ajaimueydyne|ohubyeguoqof|usowixuxaw|unoxilytuwebe|aacidiac|urunymiaha|tenkara|wikofumysuby|alobuvolytalepo|uzojyza|roewi|usuafifyfejyfez|cozeayveqic|fieacoaico|iwooxenavy|seuqaatamaqit|brightly|ilyatene|ywaram|gyjiy|obyouv|yafoeemi|vetyx|qyhozayopy|upstart|autugya|ihokiec|ebagaloyly|ybapezuboydus|swallowtail|cogive|agymynyhoiquixu|amutoocalila|yqafaepu|eazerij|ofovusex|yfopuraqis|ykucaydi|puwevixov|mega|eakeiirawiru|xisaigi|aokr|freewebportal|talent|entirety|sounds|okouvoyeruosuk|nirydyhixovox|soaoducef|fats|uhogaejelurees|tton|heofyfevudacem|irituk|nykoco|ersm|yaotyoadofyhij|owivefiuucavuv|hyysuzixoo|tininyeo|entry|cipafaxoabayzal|ruavygoxuvo|edeaq|pyrereohe|idyiwaluvub|rimezeusua|iqopynyj|afijyfiqaanieg|yvafiesyhycuaat|kibgz|accords|ocaailusiril|o4510|owygy|chmedecines|awelo|oyosv|qerunixigetu|saxuqoyxurycap|ogiqeulaxowej|ikihapyduori|abipo|resulting|viagrabuygeneric|canadianmedis|bestofmedicines|indudalatribe|pharmacy12|strabotours|medicineoftennessee|tapatio-factoria|mentabs|1medicaments|officialedmeds|cialiskaufen|amoxicillin-buy|bestedtabsinc|rx-pharmacy-links|antibioticon|searchedmeds|codmedicines|hollywoodknowitall|thewisconsinpharmacy|nevamoreunion|premiumpharmacydiscounts|rxmaury719|geomeds|canadianpharmacyzone|khalilpillsrx|themegapills|canadianmedsguide|buy-mens-pills|edrugstorehelp|mxmotomed|pharmacy130|yourppcworld|pharmacyreviewer|canadianpharmacymeds|good-bye-acne|tramadol1dollar36cent|sinmeds|onlinepharmacypricer|lifestylepharmacy|dissmedic|fiendemed|counterforlife|medicba|clayemed|rxcosmo|myidrugstore|hoodrugs|orebaremedic|medicrb|medicellatu|medicdl|medicey|drugssc|chmedecines|medalyssa|drugstoreis|mediatear|astartamed|biummmedic|medicinesi|medicinemp|cerumedic|drugstorere|rozentmedic|medicerly|drugsba|grattage|medicrgum|curragmed|generic--tools|50btc|k4912m|m94vo3|darkogard|great-oportunity|mtred|domain-crawlers|pusikuracbre|btcguild))$';

-- Store results to disk
STORE p5 INTO '/user/pdhakshi/pigouts/filter_results_month/filter_bz2_com_A.gz';
