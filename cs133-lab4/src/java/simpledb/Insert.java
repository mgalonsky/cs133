package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private DbIterator child;
    private int tableid;
    
    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        this.tid = t;
        this.child = child;
        this.tableid=tableid;
        this.hasFetched = false;
    }

    public TupleDesc getTupleDesc() {
    	Type[] fields = new Type[1];
        fields[0] = Type.INT_TYPE;
        TupleDesc td = new TupleDesc(fields);
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        this.child.open();
    }

    public void close() {
        super.close();
        this.child.close();
    }

    /**
     * You can just close and then open the child
     */
    public void rewind() throws DbException, TransactionAbortedException {
        this.child.rewind();
    }

    private boolean hasFetched;
    
    /**
     * Inserts tuples read from child into the relation with the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records (even if there are 0!). 
     * Insertions should be passed through BufferPool.insertTuple() with the 
     * TransactionId from the constructor. An instance of BufferPool is available via 
     * Database.getBufferPool(). Note that insert DOES NOT need to check to see if 
     * a particular tuple is a duplicate before inserting it.
     *
     * This operator should keep track if its fetchNext() has already been called, 
     * returning null if called multiple times.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	if(!hasFetched){
    		this.hasFetched = true;
	    	int numInserts = 0;
	        while(this.child.hasNext()){
	        	Tuple t = this.child.next();
	        	try {
					Database.getBufferPool().insertTuple(this.tid, this.tableid, t);
					numInserts+=1;
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	        }
	        Type[] fields = new Type[1];
	        fields[0] = Type.INT_TYPE;
	        TupleDesc td = new TupleDesc(fields);
	        Tuple newTuple = new Tuple(td);
	        newTuple.setField(0, new IntField(numInserts));
	        return newTuple;
    	} else{
    		return null;
    	}
    }

    @Override
    public DbIterator[] getChildren() {
        DbIterator[] children = new DbIterator[1];
        children[0] = this.child;
        return children;
    	
    }

    @Override
    public void setChildren(DbIterator[] children) {
        this.child = children[0];
    }
}
