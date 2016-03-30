package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import simpledb.Aggregator.Op;

/**
 * Computes some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    
    private int gbfield;
    private Type gbfieldtype;
    private int field;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (what != Op.COUNT) {
        	throw new IllegalArgumentException();
        }
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        
        groups = new HashMap<Field, Integer>();
    }

    
    private HashMap<Field, Integer> groups;
    private int count;
    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
    	count = 0;
    	
    	if(gbfield != NO_GROUPING && 
    			groups.containsKey(tup.getField(gbfield))){
    		count =  groups.get(tup.getField(gbfield));
    	} else if (groups.containsKey(null)) {
    		count =  groups.get(null);
    	}
    	count++;
    	
    	if (gbfield != NO_GROUPING) {
    		groups.put(tup.getField(gbfield), count);    
    	} else {
    		groups.put(null, count);    
    	}
    		
    }

    /**
     * Returns a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
ArrayList<Tuple> tuples = new ArrayList<Tuple>();
    	
    	if (gbfield == NO_GROUPING) {
    		Type[] type = new Type[1];
    		type[0] = Type.INT_TYPE;
    		TupleDesc desc = new TupleDesc(type);
    		Tuple tuple = new Tuple(desc);
    		Integer vals = groups.get(null);
    		tuple.setField(0,  new IntField(vals));
    		tuples.add(tuple);
    		TupleIterator iterToRet = new TupleIterator(desc, tuples);
        	return iterToRet;
    	}
    	
    	Type[] gbtype = new Type[2];
    	gbtype[0] = gbfieldtype;
    	gbtype[1] = Type.INT_TYPE;
    	
    	
    	
    	TupleDesc desc = new TupleDesc(gbtype);
    	
    	Iterator<Field> iter = this.groups.keySet().iterator();
    	
    	int i = 0;
    	while(iter.hasNext()) {
    		Field nextField = iter.next();
    		Tuple tup = new Tuple(desc);
    		
    		tup.setField(0, nextField);
    		Integer vals = groups.get(nextField);
    		
    		tup.setField(1, new IntField(vals));
    		tuples.add(tup);
    		
    	}
    	
    	TupleIterator iterToRet = new TupleIterator(desc, tuples);
    	return iterToRet;
    }

}
