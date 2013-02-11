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

      int queryContrib = 0;
      // Identify query type bucket
      String queryTypeBucket = "";
      if (queryType == 1 /*A record; About 66% */) {
        queryTypeBucket = "A";
        queryContrib = 66;
      } else if (queryType == 12 /*PTR record; About 15% */) {
        queryTypeBucket = "PTR";
        queryContrib = 15;
       } else if (queryType == 28 /*AAAA record; About 12% */) {
        queryTypeBucket = "AAAA";
        queryContrib = 12;
      } else /*others; 7%*/ {
        queryTypeBucket = "OTHR";
        queryContrib = 7;
      }

      // Identify TLD bucket
      int len = registeredDomain.length();
      if (registeredDomain.charAt(len-1) == '.') {
        registeredDomain = registeredDomain.substring(0, len-1);
      }
      String[] domain_parts = registeredDomain.split("\\.");
      String tld = domain_parts[(domain_parts.length)-1];

      int tldContrib = 0;
      String topLevelDomain = "";
      if (tld.equals("com") || tld.equals("COM")) /*44% */ {
        topLevelDomain = "COM";
        tldContrib = 44;
      } else if (tld.equals("net") || tld.equals("NET") /*26*/) {
        topLevelDomain = "NET";
        tldContrib = 26;
      } else if (tld.equals("arpa") || tld.equals("ARPA") /*15*/) {
        topLevelDomain = "ARPA";
        tldContrib = 15;
      } else /*15%*/ {
        topLevelDomain = "OTHR";
        tldContrib = 15;
      }

      // Identify domain bucket based on hashcode value
      long numbuckets = (long) (TOTBUCKETS* (queryContrib*tldContrib/10000.0));
      if (numbuckets == 0)
        numbuckets = 1;

      int signedHashCode = registeredDomain.hashCode();
      long unsignedHashCode = signedHashCode & 0x00000000ffffffffL;
      long bucket_id = unsignedHashCode % numbuckets;
      String domainBucketNumber = String.valueOf(bucket_id);

      // return string of type
      // '20120201_AAAA_NET_198'
      // return (inputSplit + "_" + queryTypeBucket + "_" + 
      //        topLevelDomain + "_" + domainBucketNumber);
      //return (inputSplit + "_" + "A" + "_" + 
      //        "COM" + "_" + domainBucketNumber);

      return (queryTypeBucket + "_" + topLevelDomain);
    }
}
