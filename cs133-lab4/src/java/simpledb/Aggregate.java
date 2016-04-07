package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private DbIterator child;
    private int afield;
    private int gfield;
    private Aggregator.Op aop;
    private Aggregator agg;
    private DbIterator aggIterator;
    private boolean isOpen;
    
    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of fetchNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
    	this.child = child;
    	this.afield = afield;
    	this.gfield = gfield;
    	this.aop = aop;
    	this.aggIterator = null;
    	this.isOpen = false;
    	
    	if(child.getTupleDesc().getFieldType(afield) == Type.INT_TYPE ) {
    		if(gfield == -1) {
    			this.agg = new IntegerAggregator(gfield, null, afield, aop);
    		} else{
    			this.agg = new IntegerAggregator(gfield, child.getTupleDesc().getFieldType(gfield), afield, aop);
    		}
    	} else {
    		if(gfield == -1) {
    			this.agg = new StringAggregator(gfield, null, afield, aop);
    		} else{
    			this.agg = new StringAggregator(gfield, child.getTupleDesc().getFieldType(gfield), afield, aop);
    		}
    	}
    	try {
    		child.open();
			while(child.hasNext()){
				this.agg.mergeTupleIntoGroup(child.next());
			}
		} catch (DbException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TransactionAbortedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.aggIterator = agg.iterator();
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
    	if(this.gfield == -1){
    		return Aggregator.NO_GROUPING;
    	} else {
    		return gfield;
    	}
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
    	if(this.gfield == -1){
    		return null;
    	} else {
    		return child.getTupleDesc().getFieldName(gfield);
    	}
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
		return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
    	return child.getTupleDesc().getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
		return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
    	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
		super.open();
		this.aggIterator.open();
		this.isOpen = true;
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     *
     * Hint: think about how many tuples you have to process from the child operator
     * before this method can return its first tuple.
     * Hint: notice that you each Aggregator class has an iterator() method
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	if(this.aggIterator.hasNext()){
    		Tuple next = this.aggIterator.next();
    		//System.out.println(next.toString());
    		return next;
    	} else{
    		return null;
    	}
    }

    public void rewind() throws DbException, TransactionAbortedException {
    	if(!isOpen) {
    		throw new TransactionAbortedException();
    	} else {
    		this.aggIterator.rewind();
    	}
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
		return this.aggIterator.getTupleDesc();
    }

    public void close() {
    	if(isOpen) {
    		super.close();
    		this.aggIterator.close();
    	}
    }

    /**
     * See Operator.java for additional notes
     */
    @Override
    public DbIterator[] getChildren() {
		DbIterator[] children = new DbIterator[1];
		children[0] = this.aggIterator;
		return children;
		
    }

    /**
     * See Operator.java for additional notes
     */
    @Override
    public void setChildren(DbIterator[] children) {
		this.aggIterator = children[0];
    }
    
}
