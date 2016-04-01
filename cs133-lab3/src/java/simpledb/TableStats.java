package simpledb;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private int tableid;
    private int ioCostPerPage;
    
    private int numTuples;
    
    private TupleDesc desc;
    
    private Object[] histograms; //Since both histogram types inherit directly from object we'll need to cast them after taking them out
    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
	// See project description for hint on using a Transaction
	
        this.tableid = tableid;
        this.ioCostPerPage = ioCostPerPage;
        
        Transaction t = new Transaction(); 
        t.start(); 
        SeqScan s = new SeqScan(t.getId(), tableid, "t"); 
        desc = s.getTupleDesc();
        Integer[] min;
        Integer[] max;
        min = new Integer[desc.numFields()];
        max = new Integer[desc.numFields()];
        try {
			s.open();
		} catch (DbException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (TransactionAbortedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
        
        numTuples = 0;
        // Figure out the min and max values for each int field
        try {
			while(s.hasNext()) {
				numTuples++;
				Tuple tup = s.next();
				for(int i = 0; i < desc.numFields(); ++i) {
					if(desc.getFieldType(i).equals(Type.INT_TYPE)) {
						int val = ((IntField)tup.getField(i)).getValue();
						if(min[i] == null || val < min[i]) {
							min[i] = val;
						}
						if(max[i] == null || val > max[i]) {
							max[i] = val;
						}
					}
				}
			}
		} catch (NoSuchElementException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (TransactionAbortedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (DbException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
        
        // Create the Histograms
        histograms = new Object[desc.numFields()];
        for(int i = 0; i < desc.numFields(); ++i) {
        	//Creating an IntHistogram
        	if(desc.getFieldType(i).equals(Type.INT_TYPE)) {
        		histograms[i] = new IntHistogram(NUM_HIST_BINS, min[i], max[i]);
        	} else {
        		histograms[i] = new StringHistogram(NUM_HIST_BINS);
        	}
        }
        
        //Populate the histograms
        try {
			s.rewind();
		} catch (NoSuchElementException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (DbException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (TransactionAbortedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
        try {
			while(s.hasNext()) {
				Tuple tup = s.next();
				for(int i = 0; i < desc.numFields(); ++i) {
					//Handle int fields
					if(desc.getFieldType(i).equals(Type.INT_TYPE)) {
						IntHistogram hist = (IntHistogram)histograms[i];
						hist.addValue(((IntField)tup.getField(i)).getValue());
					} else {
						StringHistogram hist = (StringHistogram)histograms[i];
						hist.addValue(((StringField)tup.getField(i)).getValue());
					}
				}
			}
		} catch (TransactionAbortedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (DbException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
        try {
			t.commit();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        //Returns the number of pages times the cost per page
    	HeapFile file = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
        return file.numPages()*ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        return (int) (numTuples*selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     *
     * Not necessary for lab 3
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        return 0.5;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
    	double selectivity;
        if(desc.getFieldType(field).equals(Type.INT_TYPE)) {
        	IntHistogram hist = (IntHistogram)histograms[field];
        	selectivity = hist.estimateSelectivity(op, ((IntField)constant).getValue());
        } else {
        	StringHistogram hist = (StringHistogram)histograms[field];
        	selectivity = hist.estimateSelectivity(op, ((StringField)constant).getValue());
        }
        return selectivity;
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        return numTuples;
    }

}
