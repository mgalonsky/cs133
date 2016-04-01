package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     *
     * Note: if the number of buckets exceeds the number of distinct integers between min and max, 
     * some buckets may remain empty (don't create buckets with non-integer widths).
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
	private int min;
	private int max;
	private int[] buckets;  //If not all buckets can have the same width, the last one will have all the overflow
	private int width;
	private int totalVals;
    public IntHistogram(int buckets, int min, int max) {
    	this.min = min;
    	this.max = max;
    	this.buckets = new int[buckets];
    	width = (max - min + 1) / buckets;
    	if (width < 1) {
    		width = 1;
    	}
    	totalVals = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	int loc = (v-min)/width;
    	loc = (loc < buckets.length? loc: buckets.length-1);
    	buckets[loc]++;
    	totalVals++;
    }

    /** 
     * Estimates the proportion of values equal to v
     * 
     * @param v
     * @return the estimate
     */
    private double equals(int v) {
    	int loc = (v-min)/width;
    	loc = (loc < buckets.length? loc: buckets.length-1);
    	return buckets[loc]/((double)width * totalVals);
    }
    
    /**
     * Estimates the number of values greater than v
     * @param v
     * @return the estimate
     */
    private double greater(int v) {
    	int loc = (v-min)/width;
    	loc = (loc < buckets.length? loc: buckets.length-1);
    	double num = buckets[loc] * ((width - 1) - ((v-min) % width)/(double)width);
		for(int i = (v - min)/width + 1; i < buckets.length; i++) {
			num += buckets[i];
		}
		return num / totalVals;
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
    	switch(op) {
    	case EQUALS:
    	case LIKE:
    		if(v < min || v > max) {
    			return 0;
    		}
    		return equals(v);
    	case GREATER_THAN:
    		if(v < min) {
    			return 1;
    		} else if (v > max) {
    			return 0;
    		}
    		return greater(v);
    	case LESS_THAN:
    		if (v < min) {
    			return 0;
    		} else if (v > max) {
    			return 1;
    		}
    		return 1 - equals(v) - greater(v);
    	case LESS_THAN_OR_EQ:
    		if (v < min) {
    			return 0;
    		} else if (v > max) {
    			return 1;
    		}
    		double temp = greater(v);
    		return 1 - temp;
    	case GREATER_THAN_OR_EQ:
    		if(v < min) {
    			return 1;
    		} else if (v > max) {
    			return 0;
    		}
    		return greater(v) + equals(v);
    	case NOT_EQUALS:
    		if(v < min || v > max) {
    			return 1;
    		}
    		return 1 - equals(v);
		default:
    		return 0.0;
    	}
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It could be used to
     *     implement a more efficient optimization
     *
     * Not necessary for lab 3
     * */
    public double avgSelectivity()
    {
        return 0.5;
    }
    
    /**
     * (Optional) A String representation of the contents of this histogram
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
