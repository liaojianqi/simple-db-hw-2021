package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.IntField;
import simpledb.storage.StringField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;
import simpledb.transaction.TransactionAbortedException;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;

/**
 * The common interface for any class that can compute an aggregate over a
 * list of Tuples.
 */
public interface Aggregator extends Serializable {
    int NO_GROUPING = -1;

    /**
     * SUM_COUNT and SC_AVG will
     * only be used in lab7, you are not required
     * to implement them until then.
     * */
    enum Op implements Serializable {
        MIN, MAX, SUM, AVG, COUNT,
        /**
         * SUM_COUNT: compute sum and count simultaneously, will be
         * needed to compute distributed avg in lab7.
         * */
        SUM_COUNT,
        /**
         * SC_AVG: compute the avg of a set of SUM_COUNT tuples,
         * will be used to compute distributed avg in lab7.
         * */
        SC_AVG;

        /**
         * Interface to access operations by a string containing an integer
         * index for command-line convenience.
         *
         * @param s a string containing a valid integer Op index
         */
        public static Op getOp(String s) {
            return getOp(Integer.parseInt(s));
        }

        /**
         * Interface to access operations by integer value for command-line
         * convenience.
         *
         * @param i a valid integer Op index
         */
        public static Op getOp(int i) {
            return values()[i];
        }
        
        public String toString()
        {
        	if (this==MIN)
        		return "min";
        	if (this==MAX)
        		return "max";
        	if (this==SUM)
        		return "sum";
        	if (this==SUM_COUNT)
    			return "sum_count";
        	if (this==AVG)
        		return "avg";
        	if (this==COUNT)
        		return "count";
        	if (this==SC_AVG)
    			return "sc_avg";
        	throw new IllegalStateException("impossible to reach here");
        }
    }

    /**
     * Merge a new tuple into the aggregate for a distinct group value;
     * creates a new group aggregate result if the group value has not yet
     * been encountered.
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    void mergeTupleIntoGroup(Tuple tup);

    /**
     * Create a OpIterator over group aggregate results.
     * @see TupleIterator for a possible helper
     */
    OpIterator iterator();

    class AggregatorIterator extends Operator {
      Map<String, Integer> aggreValues;
      Map<String, Integer> aggreValues2;
      private final int gbfield;
      private final Type gbfieldtype;
      private final Op what;
      Iterator<String> it;

      public AggregatorIterator(Map<String, Integer> aggreValues, Map<String, Integer> aggreValues2, int gbfield, Type gbfieldtype, Op what) {
        this.aggreValues = aggreValues;
        this.aggreValues2 = aggreValues2;
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.what = what;
        it = aggreValues.keySet().iterator();
      }

      @Override
      public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
        it = aggreValues.keySet().iterator();
      }

      @Override
      protected Tuple fetchNext() throws DbException, TransactionAbortedException {
        if (!it.hasNext()) return null;
        String key = it.next();
        Tuple t = new Tuple(getTupleDesc());
        if (gbfield != NO_GROUPING) {
          if (gbfieldtype == Type.INT_TYPE) {
            t.setField(0, new IntField(Integer.parseInt(key)));
          } else {
            t.setField(0, new StringField(key, 100));
          }
        }

        int val;
        if (what.equals(Op.AVG)) {
          val = aggreValues.get(key) / aggreValues2.get(key);
        } else {
          val = aggreValues.get(key);
        }
        if (gbfield != NO_GROUPING) {
          t.setField(1, new IntField(val));
        } else {
          t.setField(0, new IntField(val));
        }
        return t;
      }

      @Override
      public OpIterator[] getChildren() {
        try {
          throw new Exception("not implemented");
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public void setChildren(OpIterator[] children) {
        try {
          throw new Exception("not implemented");
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public TupleDesc getTupleDesc() {
        if (gbfield == NO_GROUPING) {
          return new TupleDesc(
              new Type[]{Type.INT_TYPE},
              new String[]{"aggregateVal"}
          );
        }
        return new TupleDesc(
            new Type[]{gbfieldtype, Type.INT_TYPE},
            new String[]{"groupVal", "aggregateVal"}
        );
      }
  }


}
