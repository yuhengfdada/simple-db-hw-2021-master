package simpledb.optimizer;

import simpledb.common.DbException;
import simpledb.execution.Predicate;

import java.util.ArrayList;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int numBuckets;
    private int min;
    private int max;

    private ArrayList<Integer> buckets;

    private int nTuples;
    private int width;
    /**
     * Create a new IntHistogram.
     * <p>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        // some code goes here
        this.numBuckets = Math.min(buckets, (max - min + 1));
        this.min = min;
        this.max = max;
        this.buckets = new ArrayList<>();
        for (int i = 0; i < numBuckets; i++) {
            this.buckets.add(0);
        }
        this.nTuples = 0;
        this.width = (max - min + 1) / numBuckets;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // some code goes here
        int index = getIndex(v);
        this.buckets.set(index, this.buckets.get(index) + 1);
        this.nTuples += 1;
    }

    private int getIndex(int v) {
        int index = (int) Math.floor(((v - min) * numBuckets) / (double) (max - min));
        return index == numBuckets ? index - 1 : index;
    }


    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        // some code goes here
        int index = getIndex(v);
        if (v > max || v < min) {
            switch (op) {
                case EQUALS:
                    return 0;
                case NOT_EQUALS:
                    return 1;
            }
        }
        if (v >= max) {
            switch (op) {
                case GREATER_THAN:
                case GREATER_THAN_OR_EQ:
                    return 0;
                case LESS_THAN:
                case LESS_THAN_OR_EQ:
                    return 1;
            }
        }
        if (v <= min) {
            switch (op) {
                case GREATER_THAN:
                case GREATER_THAN_OR_EQ:
                    return 1;
                case LESS_THAN:
                case LESS_THAN_OR_EQ:
                    return 0;
            }
        }

        int height = buckets.get(index);
        double res = 0;
        switch (op) {
            case EQUALS:
                return (height / (double) width) / nTuples;
            case NOT_EQUALS:
                return 1 - (height / (double) width) / nTuples;
            case GREATER_THAN_OR_EQ:
                res += (height / (double) width) / nTuples;
            case GREATER_THAN:
                int rightBucketEdge = min + index * width + width;
                res += (rightBucketEdge - v) / (double) width * height / nTuples;
                for (++index; index < numBuckets; index++) {
                    res += buckets.get(index) / (double) nTuples;
                }
                return res > 1 ? 1 : res;
            case LESS_THAN_OR_EQ:
                res += (height / (double) width) / nTuples;
            case LESS_THAN:
                int leftBucketEdge = min + index * width;
                res += (v - leftBucketEdge) / (double) width * height / nTuples;
                for (--index; index >= 0; index--) {
                    res += buckets.get(index) / (double) nTuples;
                }
                return res > 1 ? 1 : res;
        }
        throw new RuntimeException("shouldn't get here");
    }

    /**
     * @return the average selectivity of this histogram.
     * <p>
     * This is not an indispensable method to implement the basic
     * join optimization. It may be needed if you want to
     * implement a more efficient optimization
     */
    public double avgSelectivity() {
        // some code goes here
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return "random shit";
    }
}
