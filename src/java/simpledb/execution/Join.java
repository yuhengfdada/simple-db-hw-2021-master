package simpledb.execution;

import simpledb.storage.Field;
import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    private JoinPredicate predicate;
    private OpIterator child1Iter;
    private OpIterator child2Iter;
    private Tuple cachedChild1 = null;
    private boolean rewound = false;
    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        // some code goes here
        predicate = p;
        child1Iter = child1;
        child2Iter = child2;
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return predicate;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
        return child1Iter.getTupleDesc().getFieldName(predicate.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        return child2Iter.getTupleDesc().getFieldName(predicate.getField2());
    }

    /**
     * @see TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return TupleDesc.merge(child1Iter.getTupleDesc(),child2Iter.getTupleDesc());
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        child1Iter.open();
        child2Iter.open();
    }

    public void close() {
        // some code goes here
        child1Iter.close();
        child2Iter.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child1Iter.rewind();
        child2Iter.rewind();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        while (child1Iter.hasNext() || !rewound) {
            Tuple child1Tuple = !rewound && cachedChild1 != null ? cachedChild1 : child1Iter.next();
            cachedChild1 = child1Tuple;
            while (child2Iter.hasNext()){
                Tuple child2Tuple = child2Iter.next();
                if (predicate.filter(child1Tuple, child2Tuple)) {
                    rewound = false;
                    return merge(child1Tuple, child2Tuple);
                }
            }
            child2Iter.rewind();
            rewound = true;
        }
        return null;
    }

    private Tuple merge(Tuple t1, Tuple t2) {
        TupleDesc mergedDesc = getTupleDesc();
        Tuple mergedTuple = new Tuple(mergedDesc);

        Iterator<Field> it1 = t1.fields();
        Iterator<Field> it2 = t2.fields();

        int i = 0;
        while (it1.hasNext()) {
            mergedTuple.setField(i,t1.getField(i));
            i += 1;
            it1.next();
        }
        int j = 0;
        while (it2.hasNext()) {
            mergedTuple.setField(i, t2.getField(j));
            i += 1;
            j += 1;
            it2.next();
        }
        return mergedTuple;
    }
    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child1Iter, child2Iter};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        child1Iter = children[0];
        child2Iter = children[1];
    }

}
