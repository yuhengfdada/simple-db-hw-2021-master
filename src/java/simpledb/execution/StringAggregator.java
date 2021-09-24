package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.IntField;
import simpledb.storage.StringField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbFieldIndex;
    private Type gbFieldType;
    private int aggFieldIndex;
    private Op aggOp;

    TupleDesc td;

    private Map<Object, Integer> aggCountsMap;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (what != Op.COUNT) {
            throw new IllegalArgumentException();
        }
        gbFieldIndex = gbfield;
        gbFieldType = gbfieldtype;
        aggFieldIndex = afield;
        aggOp = what;
        aggCountsMap = new HashMap<>();

        if (gbfieldtype == null) {
            td = new TupleDesc(new Type[]{Type.INT_TYPE});
        } else {
            switch (gbfieldtype) {
                case INT_TYPE:
                    td = new TupleDesc(new Type[]{Type.INT_TYPE, Type.INT_TYPE});
                    break;
                case STRING_TYPE:
                    td = new TupleDesc(new Type[]{Type.STRING_TYPE, Type.INT_TYPE});
            }
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        // IntField aggField = (IntField) tup.getField(aggFieldIndex);
        if (gbFieldIndex == NO_GROUPING) {
            aggCountsMap.put(gbFieldIndex, aggCountsMap.getOrDefault(gbFieldIndex, 0) + 1);
        } else {
            switch (gbFieldType) {
                case INT_TYPE:
                    IntField groupField = (IntField) tup.getField(gbFieldIndex);
                    int groupFieldValue = groupField.getValue();
                    aggCountsMap.put(groupFieldValue, aggCountsMap.getOrDefault(groupFieldValue, 0) + 1);
                    break;
                case STRING_TYPE:
                    StringField stringGroupField = (StringField) tup.getField(gbFieldIndex);
                    String stringGroupFieldValue = stringGroupField.getValue();
                    aggCountsMap.put(stringGroupFieldValue, aggCountsMap.getOrDefault(stringGroupFieldValue, 0) + 1);
            }

        }
    }

    private class aggIterator implements OpIterator {
        boolean opened = false;
        StringAggregator aggregator;
        Iterator<Map.Entry<Object, Integer>> it;

        public aggIterator(StringAggregator aggregator) {
            this.aggregator = aggregator;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            opened = true;
            it = aggregator.aggCountsMap.entrySet().iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return opened && it.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (it.hasNext()) {
                Map.Entry<Object, Integer> entry = it.next();
                if (gbFieldIndex == NO_GROUPING) {
                    Tuple t = new Tuple(td);
                    t.setField(0, new IntField(entry.getValue()));
                    return t;
                } else {
                    switch (gbFieldType) {
                        case INT_TYPE:
                        Tuple t = new Tuple(td);
                        t.setField(0, new IntField((int) entry.getKey()));
                        t.setField(1, new IntField(entry.getValue()));
                        return t;
                        case STRING_TYPE:
                            Tuple ts = new Tuple(td);
                            ts.setField(0, new StringField((String)entry.getKey(), 100));
                            ts.setField(1, new IntField(entry.getValue()));
                            return ts;
                    }
                }
            }
            throw new NoSuchElementException();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            it = aggregator.aggCountsMap.entrySet().iterator();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return td;
        }

        @Override
        public void close() {
            opened = false;
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     * aggregateVal) if using group, or a single (aggregateVal) if no
     * grouping. The aggregateVal is determined by the type of
     * aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new StringAggregator.aggIterator(this);
    }

}
