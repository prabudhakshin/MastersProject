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

public class ParseDayFromFileName extends EvalFunc<String>
{
    public String exec(Tuple input) throws IOException {
      String input_file_name = (String)input.get(0);
      String[] day = input_file_name.split("\\.");
      return day[1];
    }
}
