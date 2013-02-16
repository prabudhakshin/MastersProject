package myudfs;
import java.util.Iterator;
import java.util.Set;
import java.util.HashSet;
import java.util.StringTokenizer;
import java.util.Random;
import java.lang.StringBuffer;
import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.data.DataType;
//import org.apache.pig.impl.util.WrappedIOException;

public class FindBucket extends EvalFunc<String>
{
    public String exec(Tuple input) throws IOException {

      long TOTBUCKETS = 200;
      int queryType = (Integer)input.get(0);
      String registeredDomain = (String)input.get(1);
      String inputSplitPath = (String)input.get(2);

      // Parse the date of type yyyymmdd from the input split path
      String inputSplit = "";
      String[] day = inputSplitPath.split("\\.");
      inputSplit = day[1];

      // Identify query type bucket
      String queryTypeBucket = "";
      if (queryType == 1 /*A record; About 66% */) {
        queryTypeBucket = "A";
      } else if (queryType == 12 /*PTR record; About 15% */) {
        queryTypeBucket = "PTR";
       } else if (queryType == 28 /*AAAA record; About 12% */) {
        queryTypeBucket = "AAAA";
      } else /*others; 7%*/ {
        queryTypeBucket = "OTHR";
      }

      // Identify TLD bucket
      int len = registeredDomain.length();
      if (registeredDomain.charAt(len-1) == '.') {
        registeredDomain = registeredDomain.substring(0, len-1);
      }
      String[] domain_parts = registeredDomain.split("\\.");
      String tld = domain_parts[(domain_parts.length)-1];

      String topLevelDomain = "";
      if (tld.equals("com") || tld.equals("COM")) /*44% */ {
        topLevelDomain = "COM";
      } else if (tld.equals("net") || tld.equals("NET") /*26*/) {
        topLevelDomain = "NET";
      } else if (tld.equals("arpa") || tld.equals("ARPA") /*15*/) {
        topLevelDomain = "ARPA";
      } else /*15%*/ {
        topLevelDomain = "OTHR";
      }

      String qtype_tld = queryTypeBucket + "_" + topLevelDomain;
      double percent_of_buckets_for_current_group  = 0;

      if (qtype_tld.equals("A_COM")) {
        percent_of_buckets_for_current_group = 34.418;
      } else if (qtype_tld.equals("A_NET")) {
        percent_of_buckets_for_current_group = 21.807;
      } else if (qtype_tld.equals("A_ARPA")) {
        percent_of_buckets_for_current_group = 0.129;
      } else if (qtype_tld.equals("A_OTHR")) {
        percent_of_buckets_for_current_group = 9.910;
      }

      else if (qtype_tld.equals("PTR_COM")) {
        percent_of_buckets_for_current_group = 0.108;
      } else if (qtype_tld.equals("PTR_NET")) {
        percent_of_buckets_for_current_group = 0.063;
      } else if (qtype_tld.equals("PTR_ARPA")) {
        percent_of_buckets_for_current_group = 14.869;
      } else if (qtype_tld.equals("PTR_OTHR")) {
        percent_of_buckets_for_current_group = 0.029;
      }

      else if (qtype_tld.equals("AAAA_COM")) {
        percent_of_buckets_for_current_group = 7.225;
      } else if (qtype_tld.equals("AAAA_NET")) {
        percent_of_buckets_for_current_group = 3.468;
      } else if (qtype_tld.equals("AAAA_ARPA")) {
        percent_of_buckets_for_current_group = 0.004;
      } else if (qtype_tld.equals("AAAA_OTHR")) {
        percent_of_buckets_for_current_group = 2.407;
      }

      else if (qtype_tld.equals("OTHR_COM")) {
        percent_of_buckets_for_current_group = 2.703;
      } else if (qtype_tld.equals("OTHR_NET")) {
        percent_of_buckets_for_current_group = 1.390;
      } else if (qtype_tld.equals("OTHR_ARPA")) {
        percent_of_buckets_for_current_group = 0.306;
      } else if (qtype_tld.equals("OTHR_OTHR")) {
        percent_of_buckets_for_current_group = 1.163;
      }

      // Identify domain bucket based on hashcode value
      long numbuckets = (long) (TOTBUCKETS*percent_of_buckets_for_current_group/100.0);
      if (numbuckets == 0)
        numbuckets = 1;

      int signedHashCode = registeredDomain.hashCode();
      long unsignedHashCode = signedHashCode & 0x00000000ffffffffL;
      long bucket_id = unsignedHashCode % numbuckets;
      String domainBucketNumber = String.valueOf(bucket_id);

      // System.out.println("Total buckets: " + numbuckets + "; Current bucket: " + domainBucketNumber);

      // return string of type
      // '20120201_AAAA_NET_198'
       return (inputSplit + "_" + queryTypeBucket + "_" + 
              topLevelDomain + "_" + domainBucketNumber);
    }
}
