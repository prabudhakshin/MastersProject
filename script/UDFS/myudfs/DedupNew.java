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

public class DedupNew extends EvalFunc<DataBag> {

  public DedupNew() {
    tuplefactory = TupleFactory.getInstance();
  }

  private TupleFactory tuplefactory;

  private void copyTupleFields(Tuple resulttuple, Tuple srctuple) throws Exception {
    resulttuple.set(0, (Integer)srctuple.get(0)); // ts
    resulttuple.set(1, (String)srctuple.get(1));  // src_ip
    resulttuple.set(2, (String)srctuple.get(2));  // dst_ip
    resulttuple.set(3, (String)srctuple.get(3));  // domain 
    resulttuple.set(4, (String)srctuple.get(4));  // rev_domain 
    resulttuple.set(5, (Integer)srctuple.get(5)); // qtype
    // set the ttl to be 0 for now. It will be later reset if there is a valid ttl.
    resulttuple.set(6, "N/A");  //ttl

    DataBag resulttupleanswerbag = new DefaultDataBag();
    resulttuple.set(7, resulttupleanswerbag);
    resulttuple.set(8, (String)srctuple.get(7)); // inputpath

    try {
      addAddresstoTuple(resulttuple, srctuple);
    } catch(ExecException e) {
      System.out.println("Error while retrving answer bag (6) from the input tuple: ");
      e.printStackTrace();
    }
  }

  private void addAddresstoTuple (Tuple resulttuple, Tuple srctuple) throws Exception {
    DataBag resulttupleanswerbag = (DataBag)resulttuple.get(7);
    Tuple answertuple = null;
    DataBag answerbag = (DataBag)srctuple.get(6);
    for(Tuple answer : answerbag) {
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
        resulttuple.set(6, (String) answer.get(1));
      } catch(ExecException e) {
        System.out.println("Error while retrieving field 1 (ttl) from an answer");
        e.printStackTrace();
        continue;
      }
    }

    if (answerbag.size() > 0) {
      // there is atleast one answer. So copy the dst_ip from this source tuple
      resulttuple.set(2, (String)srctuple.get(2)); //dst_ip
    }
  }

  private boolean isPartofSameRequest(int ts_current, int ts_prev, Tuple currentResultTuple) {
    int time_diff = ts_current - ts_prev;

    if (time_diff == 0) {
      return true;
    }

    if (time_diff > 3) {
      return false;
    }

    try {
      if (((String) currentResultTuple.get(6)).equals("N/A")) {
        return true;
      }
    } catch(ExecException e) {
      System.out.println("Error while retrieving field 6 (ttl) from the result tuple");
      e.printStackTrace();
      return false;
    }

    return false;
  }

  public DataBag exec(Tuple input) throws IOException {
    DataBag outputbag = new DefaultDataBag();
    Tuple prevtuple = null;
    Tuple resulttuple = tuplefactory.newTuple(9);
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
        if (isPartofSameRequest((Integer)curtuple.get(0), (Integer)prevtuple.get(0), resulttuple)) {
          addAddresstoTuple(resulttuple, curtuple);
          prevtuple = curtuple;
        }
        else {
          outputbag.add(resulttuple);
          resulttuple = tuplefactory.newTuple(9);
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
