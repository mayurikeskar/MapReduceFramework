/**
 * 
 */
package com.mr.hw.cluster.taskmanager;

import java.io.IOException;
import java.util.List;

import com.mr.hw.cluster.request.NettyRequest;

/**
 * Task manager interface 
 * @author kamlendrak
 *
 */
public interface TaskManager {

	/**
	 * Start map task on slave
	 * @param request
	 * @throws Exception
	 */
	public void startMapTask(NettyRequest request) throws Exception;
	
	/**
	 * start reduce task on slave
	 * @param request
	 * @throws Exception
	 */
	public void startReduceTask(NettyRequest request) throws Exception;
	
	/**
	 * start sort task on slave
	 * @param request
	 * @throws IOException
	 */
	public void startSort(NettyRequest request) throws IOException;
	
	/**
	 * generate samples for sort
	 * @param request
	 * @throws Exception
	 */
	public void generateSamples(NettyRequest request) throws Exception;
	
	/**
	 * get pivots from master and start partition data to appropriate slave nodes
	 * @param request
	 */
	public void startPhase2(NettyRequest request);
	
	/**
	 * sort final sort after recieving partitions
	 * @param chunksToBeRead
	 */
	public void startPhase3(List<String> chunksToBeRead);

}
