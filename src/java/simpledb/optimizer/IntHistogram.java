package simpledb.optimizer;

import simpledb.common.DbException;
import simpledb.execution.Predicate;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram implements Histogram {
    List<Integer> bucketList;
    int width;
    int min;
    int max;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.min = min;
        this.max = max;
        int maxBucket = max - min + 1;
        if (buckets > maxBucket) {
            buckets = maxBucket;
        }

        if ((max - min + 1) % buckets != 0) {
            max += buckets - (max - min + 1) % buckets;
        }

        width = (max - min + 1) / buckets;

        bucketList = new ArrayList<>();
        for (int i = 0; i < buckets; i++) {
            bucketList.add(0);
        }
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // some code goes here
        int index = (v - min) / width;
        this.bucketList.set(index, this.bucketList.get(index) + 1);
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	  // some code goes here
        if (op == Predicate.Op.EQUALS) {
            return estimateSelectivity_Equals(v);
        } else if (op == Predicate.Op.NOT_EQUALS) {
            return 1 - estimateSelectivity_Equals(v);
        } else if (op == Predicate.Op.GREATER_THAN) {
            return estimateSelectivity_GreatThan(v, false);
        } else if (op == Predicate.Op.GREATER_THAN_OR_EQ) {
            return estimateSelectivity_GreatThan(v, true);
        } else if (op == Predicate.Op.LESS_THAN) {
            return estimateSelectivity_LessThan(v, false);
        } else if (op == Predicate.Op.LESS_THAN_OR_EQ) {
            return estimateSelectivity_LessThan(v, true);
        } else {
            try {
                throw new DbException(String.format("invalid op %s", op));
            } catch (DbException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private double estimateSelectivity_Equals(int v) {
        if (v < min || v > max) {
            return 0;
        }
        int index = (v - min) / width;
        int h = this.bucketList.get(index);

        return ((double)h / width) / nTups();
    }

    private double estimateSelectivity_GreatThan(int v, boolean withEqual) {
        if (v <= min) {
            return 1;
        }
        if (withEqual) {
            if (v > max) {
                return 0;
            }
        } else {
            if (v >= max) {
                return 0;
            }
        }

        int index = (v - min) / width;
        int h = this.bucketList.get(index);

        double b_f = (double) h / nTups();
        int b_right_len = width -  (v - min) % width + 1;
        if (withEqual) b_right_len++;
        double b_part = b_right_len / (double) width;

        return b_f * b_part + getSelectivity(index + 1, this.bucketList.size() - 1);
    }

    private double estimateSelectivity_LessThan(int v, boolean withEqual) {
        if (v >= max) {
            return 1;
        }
        if (withEqual) {
            if (v < min) {
                return 0;
            }
        } else {
            if (v <= min) {
                return 0;
            }
        }

        int index = (v - min) / width;
        int h = this.bucketList.get(index);

        double b_f = (double) h / nTups();
        int b_left_len = (v - min) % width;
        if (withEqual) b_left_len++;

        double b_part = b_left_len / (double) width;

        return b_f * b_part + getSelectivity(0, index - 1);
    }

    // [s, e]
    private double getSelectivity(int s, int e) {
        s = Math.max(s, 0);
        e = Math.min(e, this.bucketList.size() - 1);
        int sum = 0;
        for (int i = s; i <= e; i++) {
            sum += this.bucketList.get(i);
        }
        return sum / (double) nTups();
    }

    private int nTups() {
        return this.bucketList.stream().reduce(0, Integer::sum);
    }

    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        StringBuffer sb = new StringBuffer();
        sb.append(String.format("min: %s \t max: %s \t width: %d\n",
            min, max, width));
        bucketList.forEach(s -> sb.append(s).append("\t"));
        sb.append("\n");
        return sb.toString();
    }
}
