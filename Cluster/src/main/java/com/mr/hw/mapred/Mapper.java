
package com.mr.hw.mapred;

import com.mr.hw.mapred.tasks.MyTaskContext;

/**
 * Mapper class
 * @author kamlendrak
 *
 */
public abstract class Mapper {
	
	public Mapper() {
		
	}
	
	/**
	 * Called once before map is called
	 * @param context
	 * @throws Exception
	 */
	public abstract void setup(MyTaskContext context) throws Exception;
	
	/**
	 * Call map function of mapper
	 * @param context
	 * @param line
	 * @throws Exception
	 */
	public abstract void map(MyTaskContext context, String line) throws Exception;
	
	/**
	 * Called once after map is complete
	 * @param context
	 * @throws Exception
	 */
	public abstract void cleanup(MyTaskContext context) throws Exception;
}
