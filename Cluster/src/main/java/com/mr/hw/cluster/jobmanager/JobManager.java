/**
 * 
 */
package com.mr.hw.cluster.jobmanager;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import com.mr.hw.cluster.request.NettyRequest;

/**
 * Job Manager interface.
 * @author kamlendrak
 *
 */
public interface JobManager {

	/**
	 * start map phase
	 * @throws Exception
	 */
	public void startMapJob() throws Exception;
	
	/**
	 * start sort phase after map phase
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public void startSort() throws FileNotFoundException, IOException;
	
	/**
	 * update status of job
	 * @param request
	 */
	public void updateStatus(NettyRequest request);
	
	/**
	 * generate pivots for sorting
	 * @param samples
	 */
	public void generatePivots(List<String> samples);
	
	/**
	 * send partitions for sorting
	 * @throws IOException
	 */
	public void sendPartitions() throws IOException;
	
	/**
	 * start reduce phase
	 * @throws Exception
	 */
	public void startReduce() throws Exception;
}
