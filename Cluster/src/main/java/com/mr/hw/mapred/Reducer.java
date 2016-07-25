package com.mr.hw.mapred;

import com.mr.hw.mapred.tasks.MyTaskContext;

/**
 * Reducer class
 * @author kamlendrak
 *
 */
public abstract class Reducer {

	/**
	 * Called once at the starting of reduce
	 * @param context
	 * @throws Exception
	 */
	public abstract void setup(MyTaskContext context) throws Exception;
	
	/**
	 * Reduce logic
	 * @param context
	 * @param key
	 * @param values
	 * @throws Exception
	 */
	public abstract void reduce(MyTaskContext context, String key, Iterable<String> values) throws Exception;
	
	/**
	 * Called once after reduce task is complete 
	 * @param context
	 * @throws Exception
	 */
	public abstract void cleanup(MyTaskContext context) throws Exception;
}
