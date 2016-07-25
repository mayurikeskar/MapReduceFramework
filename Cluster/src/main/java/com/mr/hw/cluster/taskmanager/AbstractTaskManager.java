/**
 * 
 */
package com.mr.hw.cluster.taskmanager;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;

import com.mr.hw.cluster.FileSystem.FileSystem;
import com.mr.hw.cluster.job.Job;
import com.mr.hw.cluster.job.datastructure.KeyValuePair;
import com.mr.hw.cluster.request.NettyRequest;
import com.mr.hw.cluster.request.NettyRequestType;
import com.mr.hw.mapred.tasks.MapTask;
import com.mr.hw.mapred.tasks.ReduceTask;
import com.mr.hw.mapred.tasks.SortTask;

import io.netty.channel.Channel;

/**
 * @author kamlendrak
 *
 */
public abstract class AbstractTaskManager implements TaskManager {


	private List<KeyValuePair> dataSamples = new ArrayList<KeyValuePair>();
	private List<KeyValuePair> globalSortData = new ArrayList<KeyValuePair>();

	protected final static Logger LOGGER = Logger.getLogger(AbstractTaskManager.class);

	private final int jobId;
	private final int slaveId;
	protected FileSystem fileSystem;
	protected final Channel masterChannel;
	protected Job job;

	public AbstractTaskManager(int jobId, int slaveId, Channel masterChannel, NettyRequest request) {
		this.jobId = jobId;
		this.slaveId = slaveId;
		this.job = (Job) request.getParamValue("JOB");
		this.masterChannel = masterChannel;
	}

	public void startMapTask(NettyRequest request) throws Exception {
		LOGGER.info("Starting MAP for jobId :" + jobId + "SlaveId : " + slaveId);
		try {
			MapTask task = new MapTask(request, this.slaveId, fileSystem, job, jobId);
			task.start();
			NettyRequest response = new NettyRequest(NettyRequestType.PART_MAP_DONE);
			response.setCalledId(slaveId);
			response.setJobId(jobId);
			response.setParamValue("doneFile", request.getParamValue("file"));
			this.masterChannel.writeAndFlush(response);
			LOGGER.info("sent " + NettyRequestType.PART_MAP_DONE + " for job id : " + jobId + "from slave id : " + slaveId);
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			NettyRequest errorResponse = new NettyRequest(NettyRequestType.MAP_FAILED);
			errorResponse.setJobId(request.getJobId());
			errorResponse.setCalledId(slaveId);
			masterChannel.writeAndFlush(errorResponse);
		}
	}
	
	public void startReduceTask(NettyRequest request) throws Exception {
		LOGGER.info("Starting Reduce for jobId :" + jobId + "SlaveId : " + slaveId);
		try {
			ReduceTask task = new ReduceTask(request, this.slaveId, fileSystem);
			task.start();
			//sendResponseToMaster(NettyRequestType.PART_REDUCE_DONE);
			NettyRequest response = new NettyRequest(NettyRequestType.PART_REDUCE_DONE);
			response.setCalledId(slaveId);
			response.setJobId(jobId);
			LOGGER.info("in slave"+request.getParamValue("file"));
			response.setParamValue("doneFile", request.getParamValue("file"));
			this.masterChannel.writeAndFlush(response);
			LOGGER.info("sent " + NettyRequestType.PART_REDUCE_DONE + " for job id : " + jobId + "from slave id : " + slaveId);
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			NettyRequest errorResponse = new NettyRequest(NettyRequestType.REDUCE_FAILED);
			errorResponse.setJobId(request.getJobId());
			errorResponse.setCalledId(slaveId);
			masterChannel.writeAndFlush(errorResponse);
		}
	}

	public void startSort(NettyRequest request) throws IOException{
		LOGGER.info("Sorting before Phase 1 for SlaveId : " + slaveId);
		String filePath = (String) request.getParamValue("sortFileSplit");
		String file = (String) request.getParamValue("file");
		try{
			dataSamples.addAll(fileSystem.collectLines(filePath));
			Collections.sort(dataSamples);
			NettyRequest response = new NettyRequest(NettyRequestType.PART_SORT_DONE);
			response.setCalledId(slaveId);
			response.setJobId(jobId);
			response.setParamValue("doneFile", file);
			this.masterChannel.writeAndFlush(response);
			LOGGER.info("sent " + NettyRequestType.PART_SORT_DONE + " for job id : " + jobId + "from slave id : " + slaveId);
		}catch (Exception e) {
			LOGGER.error(e.getMessage());
			NettyRequest errorResponse = new NettyRequest(NettyRequestType.PART_SORT_FAILED);
			errorResponse.setJobId(jobId);
			errorResponse.setCalledId(slaveId);
			masterChannel.writeAndFlush(errorResponse);
		}
	}
	
	public void generateSamples(NettyRequest request){
		LOGGER.info("Generating samples for SlaveId : " + slaveId);
		try{
			SortTask task = new SortTask(request, slaveId, fileSystem, job, jobId);
			List<String> samples = task.generateSamples(dataSamples);
			NettyRequest response = new NettyRequest(NettyRequestType.PIVOT_REQUEST);
			response.setParamValue("samples", samples);
			response.setJobId(jobId);
			response.setCalledId(slaveId);
			masterChannel.writeAndFlush(response);
		}catch (Exception e) {
			LOGGER.error(e.getMessage());
			NettyRequest errorResponse = new NettyRequest(NettyRequestType.PIVOT_FAILED);
			errorResponse.setJobId(jobId);
			errorResponse.setCalledId(slaveId);
			masterChannel.writeAndFlush(errorResponse);
		}
	}

	public void startPhase2(NettyRequest request){
		LOGGER.info("Creating chunks to send based on pivots for SlaveId : " + slaveId);
		try{
			SortTask task = new SortTask(request, this.slaveId, fileSystem, job, jobId);
			boolean chunksSentStatus = task.generateChunks(dataSamples); 
			//if(!chunksSentStatus) throw new Exception();
			sendResponseToMaster(NettyRequestType.PARTITION_REQUEST);
		}catch (Exception e) {
			LOGGER.error(e.getMessage());
			NettyRequest errorResponse = new NettyRequest(NettyRequestType.PARTITION_REQUEST_FAILED);
			errorResponse.setJobId(jobId);
			errorResponse.setCalledId(slaveId);
			masterChannel.writeAndFlush(errorResponse);
		}
	}

	public void startPhase3(List<String> chunksToBeRead){
		LOGGER.info("Merging the chunks, creating final sorted list : " + slaveId);
		try{
			File file  = new File("partitions");
			for(File f : file.listFiles()){
				globalSortData.addAll(fileSystem.collectLines("partitions/"+f.getName()));
			}
			Collections.sort(globalSortData);
			
			fileSystem.writeToKeyFolder(globalSortData);
			File reduceFolder = new File("reduceInput");
			/*for(File f : reduceFolder.listFiles())
				SftpTransferManager.sftpMain(f.getName(), "reduceInput");
			*/sendResponseToMaster(NettyRequestType.SORT_DONE);
		}catch (Exception e) {
			LOGGER.error(e.getMessage());
			NettyRequest errorResponse = new NettyRequest(NettyRequestType.SORT_FAILED);
			errorResponse.setJobId(jobId);
			errorResponse.setCalledId(slaveId);
			masterChannel.writeAndFlush(errorResponse);
		}
	}
	
	private void sendResponseToMaster(NettyRequestType type) {
		NettyRequest request = new NettyRequest(type);
		request.setCalledId(slaveId);
		request.setJobId(jobId);
		this.masterChannel.writeAndFlush(request);
		LOGGER.info("sent " + type + " for job id : " + jobId + "from slave id : " + slaveId);
	}
}
