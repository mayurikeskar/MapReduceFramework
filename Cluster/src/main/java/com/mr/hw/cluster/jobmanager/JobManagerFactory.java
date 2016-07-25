/**
 * 
 */
package com.mr.hw.cluster.jobmanager;

import java.util.Map;

import com.mr.hw.cluster.request.NettyRequest;

import io.netty.channel.Channel;

/**
 * Factory class to get the appropriate job manager.
 * @author kamlendrak
 *
 */
public class JobManagerFactory {

	/**
	 * Gets the job manager instance either aws or local depending on the mode of execution
	 * @param mode
	 * @param jobId
	 * @param request
	 * @param slaveChannels
	 * @return
	 * @throws Exception
	 */
	public static JobManager getJobManager(String mode, int jobId, NettyRequest request,
			Map<Integer, Channel> slaveChannels) throws Exception {
		return "aws".equalsIgnoreCase(mode) ? new AWSMRJobManager(jobId, request, slaveChannels)
				: new LocalMRJobManager(jobId, request, slaveChannels);
	}
}
