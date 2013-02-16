package myudfs;
import java.util.Iterator;
import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultDataBag;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.backend.executionengine.ExecException;
//import org.apache.pig.impl.util.WrappedIOException;

public class Dedup extends EvalFunc<DataBag> {

  public Dedup() {
    tuplefactory = TupleFactory.getInstance();
  }

  private TupleFactory tuplefactory;

  private void copyTupleFields(Tuple resulttuple, Tuple srctuple) throws Exception {
    resulttuple.set(0, (Integer)srctuple.get(0)); // ts
    resulttuple.set(1, (String)srctuple.get(1));  // src_ip
    resulttuple.set(2, (String)srctuple.get(2));  // dst_ip
    resulttuple.set(3, (String)srctuple.get(3));  // domain 
    // set the ttl to be 0 for now. It will be later reset if there is a valid ttl.
    resulttuple.set(4, "0");  //ttl

    DataBag resulttupleanswerbag = new DefaultDataBag();
    resulttuple.set(5, resulttupleanswerbag);

    try {
      addAddresstoTuple(resulttuple, (DataBag)srctuple.get(4));
    } catch(ExecException e) {
      System.out.println("Error while retrving answer bag (4) from the input tuple: ");
      e.printStackTrace();
    }
  }

  private void addAddresstoTuple (Tuple resulttuple, DataBag answerbag) throws Exception {
    DataBag resulttupleanswerbag = (DataBag)resulttuple.get(5);
    Tuple answertuple = null;
    for(Tuple answer : (DataBag)answerbag) {
      answertuple = tuplefactory.newTuple(1);
      try {
        answertuple.set(0, (String)answer.get(4));
      } catch(ExecException e) {
        System.out.println("Error while retrieving field 4 from an answer");
        e.printStackTrace();
        continue;
      }
      resulttupleanswerbag.add(answertuple);

      //set the ttl field of the resulttuple
      try {
        resulttuple.set(4, (String) answer.get(1));
      } catch(ExecException e) {
        System.out.println("Error while retrieving field 1 (ttl) from an answer");
        e.printStackTrace();
        continue;
      }
    }
  }

  public DataBag exec(Tuple input) throws IOException {
    DataBag outputbag = new DefaultDataBag();
    Tuple prevtuple = null;
    /*Fields of tuple: ts, src_ip, dst_ip, domain, ttl, answer*/
    Tuple resulttuple = tuplefactory.newTuple(6);
    DataBag inputbag = (DataBag)input.get(0);
    if (inputbag.size() == 0) {
      System.out.println("Bag does not have any tuples. Returning null");
      return null;
    }
    Iterator<Tuple> itertuple = inputbag.iterator();
    prevtuple = itertuple.next();

    try {
      copyTupleFields(resulttuple /*dest*/, prevtuple /*src*/);
    } catch (Exception e) {
      System.out.println("Exception while processing first tuple. Skipping process");
      e.printStackTrace();
      return outputbag;
    }

    while(itertuple.hasNext()) {
      try{
        Tuple curtuple = itertuple.next();
        if (((Integer)curtuple.get(0) - (Integer)prevtuple.get(0)) <= 5) {
          DataBag answerbag = (DataBag)curtuple.get(4);
          addAddresstoTuple(resulttuple, answerbag);
          prevtuple = curtuple;
        }
        else {
          outputbag.add(resulttuple);
          resulttuple = tuplefactory.newTuple(6);
          prevtuple = curtuple;
          copyTupleFields(resulttuple /*dest*/, prevtuple /*src*/);
        }
      } catch (Exception e) {
          System.out.println("Exception while processing a tuple. Skipping the tuple");
          e.printStackTrace();
      }
    }
    outputbag.add(resulttuple);
    return outputbag;
  }
}
