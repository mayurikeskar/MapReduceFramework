
package com.mr.hw.cluster.taskmanager;

import org.apache.log4j.Logger;

import com.mr.hw.cluster.request.NettyRequest;

import io.netty.channel.Channel;

/**
 * Gets appropriate task manager for slaves based on the mode of execution either local or aws
 * @author kamlendrak
 *
 */
public class TaskManagerFactory {
	
	private static final Logger LOGGER = Logger.getLogger(TaskManagerFactory.class);

	public static TaskManager getTaskManager(String mode, int jobId, int slaveId, Channel masterChannel, NettyRequest request)
			throws Exception {
		LOGGER.info("create task manager for jobId : " + jobId + " at slaveId : " + slaveId);
		return "aws".equalsIgnoreCase(mode) ? new AWSTaskManager(jobId, slaveId, masterChannel, request)
				: new LocalTaskManager(jobId, slaveId, masterChannel, request);
	}
}
