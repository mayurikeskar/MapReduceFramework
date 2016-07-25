
package com.mr.hw.cluster.taskmanager;

import com.mr.hw.cluster.FileSystem.LocalfileSystem;
import com.mr.hw.cluster.request.NettyRequest;

import io.netty.channel.Channel;

/**
 * Task manager for running task locally
 * @author kamlendrak
 *
 */
public class LocalTaskManager extends AbstractTaskManager {

	public LocalTaskManager(int jobId, int slaveId, Channel masterChannel, NettyRequest request) {
		super(jobId, slaveId, masterChannel, request);
		fileSystem = new LocalfileSystem();
	}

	public void generateSamples() {
		generateSamples();
	}
}
