package myudfs;
import java.util.Iterator;
import java.util.Set;
import java.util.HashSet;
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

public class MyUDF extends EvalFunc<Tuple>
{
    public Tuple exec(Tuple input) throws IOException {
      Tuple group_filename_ip = (Tuple)input.get(0);
      String filename = (String)group_filename_ip.get(0);
      String ip = (String)group_filename_ip.get(1);

      //System.out.println("<<<<<<FileName>>>>" + filename);
      //System.out.println("<<<<<<IP>>>>" + ip);

      //Iterator<Tuple> domainList = ((DataBag)input.get(1)).iterator();
      //
      DataBag domainList = (DataBag)input.get(1);
      Set<String> unique_domains = new HashSet<String>();
      for (Tuple aDomain : domainList) {
        //System.out.println("<<<Tuple>>" + aDomain.toDelimitedString("+"));
        String domain_first_char = (String)aDomain.get(1);
        //System.out.println("<<FirstChar>>" + domain_first_char);
        unique_domains.add(domain_first_char);
      }

      //System.out.println("<<SIZE OF UNIQ DOMS>> " + unique_domains.size());

      StringBuffer strbuffer = new StringBuffer();
      //Iterator<String> domains = unique_domains.iterator();

      for (String aDomain : unique_domains) {
        //System.out.println("<<domain_first_char>> " + aDomain);
        strbuffer.append(aDomain + " ");
        //System.out.println("<<str buffer>>" + strbuffer.toString());
      }

      String space_separated_domain_first_char = null;

      if (strbuffer.length() != 0)
        space_separated_domain_first_char = strbuffer.substring(0, strbuffer.length()-1);

      Tuple result_tuple = TupleFactory.getInstance().newTuple(3);
      result_tuple.set(0, filename);
      result_tuple.set(1, ip);
      result_tuple.set(2, space_separated_domain_first_char);
      //result_tuple.append(filename);
      //result_tuple.append(ip);
      //result_tuple.append(space_separated_domain_first_char);

      return result_tuple;

    }

    // public Schema outputSchema(Schema input) {
    //     try{
    //         Schema tupleSchema = new Schema();
    //         tupleSchema.add(new Schema.FieldSchema("filename", DataType.CHARARRAY));
    //         tupleSchema.add(new Schema.FieldSchema("ip", DataType.CHARARRAY));
    //         tupleSchema.add(new Schema.FieldSchema("domain_buckets", DataType.CHARARRAY));
    //         return new Schema(new Schema.FieldSchema("mySchema",tupleSchema, DataType.TUPLE));
    //     } catch (Exception e){
    //             System.out.println("<<< GOT EXCEPTION IN OUTPUT SCHEMA >>>");
    //             return null;
    //     }
    // }

}
