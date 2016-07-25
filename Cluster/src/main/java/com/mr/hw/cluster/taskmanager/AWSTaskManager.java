/**
 * 
 */
package com.mr.hw.cluster.taskmanager;

import com.mr.hw.cluster.FileSystem.AWSFileSystem;
import com.mr.hw.cluster.request.NettyRequest;

import io.netty.channel.Channel;

/**
 * Task manager for aws
 * @author kamlendrak
 *
 */
public class AWSTaskManager extends AbstractTaskManager {

	public AWSTaskManager(int jobId, int slaveId, Channel masterChannel, NettyRequest request) {
		super(jobId, slaveId, masterChannel, request);
		fileSystem = new AWSFileSystem();
	}

	public void startMapTask(NettyRequest request) throws Exception {
		
	}

	public void generateSamples() {
		
	}
}
