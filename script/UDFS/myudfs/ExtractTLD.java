package myudfs;
import java.util.Iterator;
import java.util.Set;
import java.util.HashSet;
import java.util.StringTokenizer;
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

public class ExtractTLD extends EvalFunc<String>
{
    public String exec(Tuple input) throws IOException {
      String registeredDomain = (String)input.get(0);
      int len = registeredDomain.length();
      if (registeredDomain.charAt(len-1) == '.') {
        registeredDomain = registeredDomain.substring(0, len-1);
      }
      String[] domain_parts = registeredDomain.split("\\.");
      return domain_parts[(domain_parts.length)-1];
    }
}
