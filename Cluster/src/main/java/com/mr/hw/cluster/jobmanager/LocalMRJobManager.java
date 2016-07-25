/**
 * 
 */
package com.mr.hw.cluster.jobmanager;

import java.util.Map;

import com.mr.hw.cluster.FileSystem.LocalfileSystem;
import com.mr.hw.cluster.job.Job;
import com.mr.hw.cluster.request.NettyRequest;

import io.netty.channel.Channel;

/**
 * Job Manager for running jobs in local mode.
 * @author kamlendrak
 *
 */
public class LocalMRJobManager extends MRJobManager {
	
	public LocalMRJobManager(int jobId, NettyRequest request, Map<Integer, Channel> slaveChannels) throws Exception {
		super(jobId, request, slaveChannels);
		this.fs = new LocalfileSystem();
		this.job = (Job) request.getParamValue("JOB");
		fileOperationStatus = fs.accessInput(job.getInputPath());
		validateJobDetails();
	}

	/**
	 * Validates the job details.
	 * @throws Exception
	 */
	private void validateJobDetails() throws Exception {
		String inputPath = this.job.getInputPath();
		String outputpath = this.job.getOuputPath();
		if(inputPath == null || inputPath.length() == 0 || 
				outputpath == null || outputpath.length() == 0) {
			throw new Exception("Input/Output path not given");
		}
		fs.checkOutputFolder(outputpath, ""/*bucket name in case of aws*/);
		fs.checkInputFolder(inputPath, "");
	}

}
