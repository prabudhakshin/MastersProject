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

public class ExtractQtypeTLD extends EvalFunc<Tuple>
{
    public Tuple exec(Tuple input) throws IOException {

      int queryType = (Integer)input.get(0);
      String revRegisteredDomain = (String)input.get(1);

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
      String[] domain_parts = revRegisteredDomain.split("\\.");
      String tld = domain_parts[0];

      String topLevelDomain = "";
      if (tld.equals("com") || tld.equals("COM")) /*44% */ {
        topLevelDomain = "COM";
      } else if (tld.equals("net") || tld.equals("NET") /*26*/) {
        topLevelDomain = "NET";
      } else if (tld.equals("arpa") || tld.equals("ARPA") /*15*/) {
        topLevelDomain = "ARPA";
      } else if (tld.equals("org") || tld.equals("ORG") /*3*/) {
        topLevelDomain = "ORG";
      } else /*12%*/ {
        topLevelDomain = "OTHR";
      }

      String qtype_tld = queryTypeBucket + "_" + topLevelDomain;

      TupleFactory tuplefactory = TupleFactory.getInstance();
      Tuple resulttuple = tuplefactory.newTuple(3);

      resulttuple.set(0, (Integer) queryType);
      resulttuple.set(1, (String) tld);
      resulttuple.set(2, (String) qtype_tld);
      return resulttuple;
    }
}
