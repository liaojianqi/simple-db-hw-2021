package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.StringField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.sql.PseudoColumnUsage;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;

    Map<String, Integer> aggreValues;
    Map<String, Integer> aggreValues2;


    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;

        this.aggreValues = new HashMap<>();
        this.aggreValues2 = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field gbField = gbfield == NO_GROUPING ? new StringField("NO_GROUPING", 100) : tup.getField(gbfield);
        IntField aField = (IntField)tup.getField(afield);
        String key = gbField.toString();

        if (!this.aggreValues.containsKey(key)) {
            this.aggreValues.put(key, aField.getValue());
            if (what.equals(Op.COUNT)) {
                this.aggreValues.put(key, 1);
            }
            if (what.equals(Op.AVG)) {
                this.aggreValues2.put(key, 1);
            }
            return ;
        }
        Integer curValue = this.aggreValues.get(key);
        int newValue = aField.getValue();
        switch (what) {
            case MIN:
                curValue = Math.min(curValue, newValue);
                this.aggreValues.put(key, curValue);
                break;
            case MAX:
                curValue = Math.max(curValue, newValue);
                this.aggreValues.put(key, curValue);
                break;
            case SUM:
                curValue += newValue;
                this.aggreValues.put(key, curValue);
                break;
            case COUNT:
                curValue++;
                this.aggreValues.put(key, curValue);
                break;
            case AVG:
                curValue += newValue;
                this.aggreValues.put(key, curValue);
                this.aggreValues2.put(key, this.aggreValues2.get(key) + 1);
                break;
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new AggregatorIterator(aggreValues, aggreValues2, gbfield, gbfieldtype, what);
    }
}
