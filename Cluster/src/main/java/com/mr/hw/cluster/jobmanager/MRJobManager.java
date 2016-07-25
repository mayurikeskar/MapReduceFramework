package com.mr.hw.cluster.jobmanager;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.mr.hw.cluster.FileSystem.FileSystem;
import com.mr.hw.cluster.job.Job;
import com.mr.hw.cluster.request.NettyRequest;
import com.mr.hw.cluster.request.NettyRequestType;
import com.mr.hw.mapred.job.status.FileOperationStatus;
import com.mr.hw.mapred.job.status.JobStatus;
import com.mr.hw.mapred.job.status.SlaveStatus;
import com.mr.hw.mapred.tasks.SortTask;

import io.netty.channel.Channel;

/**
 * Abstract class to handle the common job manager part.
 * @author kamlendrak
 *
 */
public abstract class MRJobManager implements JobManager {

	protected static final Logger LOGGER = Logger.getLogger(MRJobManager.class);

	protected final int jobId;
	protected final Map<Integer, Channel> slaveChannels;
	protected FileSystem fs;
	protected JobStatus jobStatus;
	protected Job job;

	private Map<Integer, SlaveStatus> slaveStatusMap;
	protected Map<String, FileOperationStatus> fileOperationStatus;

	private static int mapTaskCount = 0;
	private static int reduceTaskCount = 0;

	public MRJobManager(int jobId, NettyRequest request, Map<Integer, Channel> slaveChannels) throws IOException {
		LOGGER.info("creating job with 1 master and " + slaveChannels.size() + " slaves");
		this.slaveChannels = slaveChannels;
		this.jobId = jobId;
		this.job = (Job) request.getParamValue("JOB");
		slaveStatusMap = new HashMap<Integer, SlaveStatus>();
		setDefaultSlaveStatus();
	}

	/**
	 * Starts the MR job
	 * 
	 * @throws Exception
	 */
	public void startMapJob() throws Exception {
		this.jobStatus = JobStatus.RUNNING_MAP;
		for (Entry<Integer, SlaveStatus> entry : slaveStatusMap.entrySet()) {
			SlaveStatus status = entry.getValue();
			if (status == SlaveStatus.IDLE || status == SlaveStatus.PART_MAP_DONE) {
				for (Entry<String, FileOperationStatus> file : fileOperationStatus.entrySet()) {
					FileOperationStatus fileStatus = file.getValue();
					if (fileStatus == FileOperationStatus.NOT_STARTED || fileStatus == FileOperationStatus.FAILED) {
						startMapTaskOnSlave(entry.getKey(), file.getKey());
						break;
					}
				}
			}
		}

		Collection<FileOperationStatus> status = fileOperationStatus.values();
		boolean mapInProgress = status.contains(FileOperationStatus.NOT_STARTED) || 
				status.contains(FileOperationStatus.FAILED) || status.contains(FileOperationStatus.IN_PROGRESS);
		if(!mapInProgress){
			fileOperationStatus.clear();
			fileOperationStatus = fs.accessInput("map");
			startSort();
		}
	}


	/**
	 * Sends one file at a time to the designated slave to start the map task
	 * 
	 * @param slaveId
	 * @param file
	 */
	private void startMapTaskOnSlave(int slaveId, String file) {
		LOGGER.info("Sending map request to slave : " + slaveId + " for file : " + file);
		NettyRequest request = new NettyRequest(NettyRequestType.START_MAP);
		request.setParamValue("file", file);
		request.setJobId(jobId);
		request.setParamValue("taskId", ++mapTaskCount);
		request.setParamValue("JOB", job);
		Channel slaveChannel = slaveChannels.get(slaveId);
		if (slaveChannel != null) {
			slaveChannel.writeAndFlush(request);
			LOGGER.info("sent");
		}
		fileOperationStatus.put(file, FileOperationStatus.IN_PROGRESS);
		slaveStatusMap.put(slaveId, SlaveStatus.MAP_RUNNING);
	}

	/**
	 * Start the distributed sort phase.
	 */
	public void startSort() throws FileNotFoundException, IOException {
		this.jobStatus = JobStatus.RUNNING_SORT;
		LOGGER.info("Distributing the files and starting sort : ");
		for (Entry<Integer, SlaveStatus> entry : slaveStatusMap.entrySet()) {
			SlaveStatus status = entry.getValue();
			if (status == SlaveStatus.PART_SORT_DONE || status == SlaveStatus.PART_MAP_DONE) {
				for (Entry<String, FileOperationStatus> file : fileOperationStatus.entrySet()) {
					FileOperationStatus fileStatus = file.getValue();
					if (fileStatus == FileOperationStatus.NOT_STARTED || fileStatus == FileOperationStatus.FAILED) {
						//	boolean fileTransferStatus = sendFilesToSort(entry.getKey(), file.getKey());
						//	if (fileTransferStatus)
						startSortTaskOnSlave(entry.getKey(), file.getKey());
						break;
					} 
				}
			}
		}
		Collection<FileOperationStatus> status = fileOperationStatus.values();
		boolean mapInProgress = status.contains(FileOperationStatus.NOT_STARTED) || 
				status.contains(FileOperationStatus.FAILED) || status.contains(FileOperationStatus.IN_PROGRESS);
		if(!mapInProgress){
			fileOperationStatus.clear();
			for (Entry<Integer, SlaveStatus> entry : slaveStatusMap.entrySet()) {
				startPhase1(entry.getKey());
			}
		}
	}

	/**
	 * Send partitions to slave nodes.
	 */
	public void sendPartitions() throws IOException {
		this.jobStatus = JobStatus.PHASE3;
		LOGGER.info("Sending partition files to respective slaves: ");
		for (Entry<Integer, SlaveStatus> entry : slaveStatusMap.entrySet()) {
			Channel slaveChannel = slaveChannels.get(entry.getKey());
			if (slaveChannel != null)
				sendFiles(entry.getKey(), slaveChannel);
			slaveStatusMap.put(entry.getKey(), SlaveStatus.PHASE3);
		}
	}

	/**
	 * Send distributed sort phase 3 request to slaves. 
	 * @param slaveId
	 * @param slaveChannel
	 * @throws IOException
	 */
	private void sendFiles(int slaveId, Channel slaveChannel) throws IOException {
		List<String> partitionList = fs.getPartitionFiles(slaveId, "partitions");
		LOGGER.info("Files have been sent to slave");
		NettyRequest request = new NettyRequest(NettyRequestType.PHASE3);
		request.setJobId(jobId);
		request.setParamValue("partitionList", partitionList);
		slaveChannel.writeAndFlush(request);
	}

	/**
	 * Sends slaves the request to start Distributed sort phase 1.
	 * @param slaveId
	 */
	private void startPhase1(int slaveId) {
		this.jobStatus = JobStatus.PHASE1;
		LOGGER.info("Starting Phase 1 on: " + slaveId);
		NettyRequest request = new NettyRequest(NettyRequestType.PHASE1);
		request.setJobId(jobId);
		request.setParamValue("slaveCount", slaveStatusMap.size());
		Channel slaveChannel = slaveChannels.get(slaveId);
		if (slaveChannel != null) {
			slaveChannel.writeAndFlush(request);
			LOGGER.info("Files have been sent to slave");
		}
		slaveStatusMap.put(slaveId, SlaveStatus.PHASE1);
	}

	/**
	 * Start sort on slaves.
	 * @param slaveId
	 * @param file
	 */
	private void startSortTaskOnSlave(int slaveId, String file) {
		LOGGER.info("Sending sort files to slave : " + slaveId + " for file : " + file);
		NettyRequest request = new NettyRequest(NettyRequestType.START_SORT);
		request.setJobId(jobId);
		request.setParamValue("sortFileSplit", "map/"+file);
		request.setParamValue("file", file);
		Channel slaveChannel = slaveChannels.get(slaveId);
		if (slaveChannel != null) {
			slaveChannel.writeAndFlush(request);
			LOGGER.info("Sent the sort file to: " + slaveId);
		}
		fileOperationStatus.put(file, FileOperationStatus.IN_PROGRESS);
		slaveStatusMap.put(slaveId, SlaveStatus.SORT_RUNNING);
	}

	/**
	 * Sends the request to generate pivots.
	 */
	public void generatePivots(List<String> samples) {
		List<String> pivots = SortTask.generatePivots(samples, slaveStatusMap.size());
		this.jobStatus = JobStatus.PHASE2;
		LOGGER.info("Sending pivots to all the slaves: ");
		for (Entry<Integer, SlaveStatus> entry : slaveStatusMap.entrySet()) {
			NettyRequest request = new NettyRequest(NettyRequestType.PHASE2);
			request.setJobId(jobId);
			request.setParamValue("pivots", pivots);
			request.setParamValue("slaveCount", slaveStatusMap.size());
			Channel slaveChannel = slaveChannels.get(entry.getKey());
			if (slaveChannel != null) {
				slaveChannel.writeAndFlush(request);
				LOGGER.info("Sent the pivots to: " + entry.getKey());
				slaveStatusMap.put(entry.getKey(), SlaveStatus.PHASE2);
			}
		}
	}

	/**
	 * Sends default status for slaves.
	 */
	private void setDefaultSlaveStatus() {
		for (Entry<Integer, Channel> entry : slaveChannels.entrySet()) {
			slaveStatusMap.put(entry.getKey(), SlaveStatus.IDLE);
		}
	}

	/**
	 * Starts the reduce job.
	 */
	public void startReduce() throws Exception {
		if(fileOperationStatus.isEmpty())
			fileOperationStatus = fs.accessInput("reduceInput");
		this.jobStatus = JobStatus.RUNNING_REDUCE;
		for (Entry<Integer, SlaveStatus> entry : slaveStatusMap.entrySet()) {
			SlaveStatus status = entry.getValue();
			if (status == SlaveStatus.PHASE3 || status == SlaveStatus.PART_REDUCE_DONE) {
				for (Entry<String, FileOperationStatus> file : fileOperationStatus.entrySet()) {
					FileOperationStatus fileStatus = file.getValue();
					if (fileStatus == FileOperationStatus.NOT_STARTED || fileStatus == FileOperationStatus.FAILED) {
						startReduceTaskOnSlave(entry.getKey(), file.getKey());
						break;
					} 
				}
			}
		}
	}

	/**
	 * Sends the request to slave to start the reduce job.
	 * @param slaveId
	 * @param file
	 * @throws IOException 
	 */
	private void startReduceTaskOnSlave(int slaveId, String file) throws IOException {
		LOGGER.info("Sending reduce request to slave : " + slaveId + " for file : " + file);
		NettyRequest request = new NettyRequest(NettyRequestType.START_REDUCE);
		request.setParamValue("file", file);
		request.setJobId(jobId);
		request.setParamValue("JOB", job);
		request.setParamValue("taskId", ++reduceTaskCount);
		Channel slaveChannel = slaveChannels.get(slaveId);
		if (slaveChannel != null) {
			slaveChannel.writeAndFlush(request);
			LOGGER.info("sent");
		}
		fileOperationStatus.put(file, FileOperationStatus.IN_PROGRESS);
		slaveStatusMap.put(slaveId, SlaveStatus.REDUCE_RUNNING);
	}

	/**
	 * Update request has been received from slave. 
	 * Notify the job.
	 */
	public void updateStatus(NettyRequest request) {
		LOGGER.info("in master"+request.getParamValue("doneFile"));
		fileOperationStatus.put((String) request.getParamValue("doneFile"), FileOperationStatus.DONE);
		SlaveStatus status = null;
		if (request.getType() == NettyRequestType.PART_MAP_DONE)
			status = SlaveStatus.PART_MAP_DONE;
		if (request.getType() == NettyRequestType.PART_SORT_DONE)
			status = SlaveStatus.PART_SORT_DONE;
		if (request.getType() == NettyRequestType.PART_REDUCE_DONE)
			status = SlaveStatus.PART_REDUCE_DONE;
		slaveStatusMap.put((Integer) request.getCalledId(), status);
	}
}