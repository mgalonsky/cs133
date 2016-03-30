package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    
    private TransactionId tid;
    private DbIterator child;
    private boolean hasBeenDeleated;
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        tid = t;
        this.child = child;
        hasBeenDeleated = false;
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
        child.close();
    }
    /**
     * You can just close and then open the child
     */
    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method. You can pass along the TransactionId from the constructor.
     * This operator should keep track of whether its fetchNext() method has been called already. 
     * 
     * @return A 1-field tuple containing the number of deleted records (even if there are 0)
     *          or null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	if(!hasBeenDeleated){
    		this.hasBeenDeleated = true;
	    	int numDeletes = 0;
	        while(this.child.hasNext()){
	        	Tuple t = this.child.next();
	        	try {
					Database.getBufferPool().deleteTuple(this.tid, t);
					numDeletes+=1;
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	        }
	        Type[] fields = new Type[1];
	        fields[0] = Type.INT_TYPE;
	        TupleDesc td = new TupleDesc(fields);
	        Tuple newTuple = new Tuple(td);
	        newTuple.setField(0, new IntField(numDeletes));
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
