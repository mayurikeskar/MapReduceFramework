/**
 * 
 */
package com.mr.hw.cluster.jobmanager;

import java.io.IOException;
import java.util.Map;

import com.mr.hw.cluster.FileSystem.AWSFileSystem;
import com.mr.hw.cluster.job.Job;
import com.mr.hw.cluster.request.NettyRequest;

import io.netty.channel.Channel;

/**
 * Job manager for AWS jobs
 * @author kamlendrak
 *
 */
public class AWSMRJobManager extends MRJobManager {
	
	public AWSMRJobManager(int jobId, NettyRequest request, Map<Integer, Channel> slaveChannels) throws IOException {
		super(jobId, request, slaveChannels);
		this.fs = new AWSFileSystem();
		this.job = (Job) request.getParamValue("JOB");
		fileOperationStatus = fs.accessInput(job.getInputPath());
	}	
}
