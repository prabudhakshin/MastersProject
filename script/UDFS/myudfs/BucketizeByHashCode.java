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

public class BucketizeByHashCode extends EvalFunc<String>
{
    public String exec(Tuple input) throws IOException {
      long numbuckets = 10;
      String registered_domain = (String)input.get(0);
      int signedHashCode = registered_domain.hashCode();
      long unsignedHashCode = signedHashCode & 0x00000000ffffffffL;
      long bucket_id = unsignedHashCode % numbuckets;
      //int ran = new Random().nextInt(676);
      //String hashbucket = String.valueOf(ran);
      String hashbucket = String.valueOf(bucket_id);
      return hashbucket; 
    }
}
