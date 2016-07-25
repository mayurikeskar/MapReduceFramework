package com.mr.hw.cluster.master;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.mr.hw.cluster.jobmanager.JobManager;
import com.mr.hw.cluster.jobmanager.JobManagerFactory;
import com.mr.hw.cluster.request.NettyRequest;
import com.mr.hw.cluster.request.NettyRequestType;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * Manages job requests to and from masters 
 * @author kamlendrak
 *
 */
public class MasterChannelhandler extends ChannelInboundHandlerAdapter {

	private static final Logger LOGGER = Logger.getLogger(MasterChannelhandler.class);

	private static Map<Integer, Channel> slaveChannels = new HashMap<Integer, Channel>();
	private static Map<Integer, JobManager> jobIdJobManagerMap = new HashMap<Integer, JobManager>();
	private static List<String> samples = new ArrayList<String>();

	private static int slaveCounter = 0;
	private static int partitionCounter = 0;
	private static int jobCounter = 0;
	private static int sortCounter = 0;
	private static String jobMode;

	public MasterChannelhandler(String mode) {
		jobMode = mode;
	}

	@Override
	public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
	}

	@Override
	public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
	}

	/**
	 * calls appropriate job task depending on the request
	 */
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		NettyRequest request = (NettyRequest) msg;
		LOGGER.info(request.getType() + " : " + request.getCalledId());

		if (request.getType() == NettyRequestType.SLAVE_REGISTRATION) {
			registerSlaveChannel(ctx.channel());
		}

		if (request.getType() == NettyRequestType.JOB_SUBMISSION) {
			startMapJob(++jobCounter, request);
		}
		if (request.getType() == NettyRequestType.PART_MAP_DONE) {
			updateStatus(request);
		}
		if (request.getType() == NettyRequestType.PART_SORT_DONE) {
			updateStatus(request);
		}
		
		if (request.getType() == NettyRequestType.PART_REDUCE_DONE) {
			updateStatus(request);
		}
		
		
		if (request.getType() == NettyRequestType.PIVOT_REQUEST) {
			samples.addAll((List) request.getParamValue("samples"));
			if (samples.size() == slaveCounter * slaveCounter)
				generatePivots(request.getJobId(), samples);
		}

		if (request.getType() == NettyRequestType.PARTITION_REQUEST) {
			partitionCounter++;
			if (partitionCounter == slaveCounter) {
				sendPartitions(request.getJobId());
			}
		}
		if (request.getType() == NettyRequestType.SORT_DONE) {
			sortCounter++;
			if (sortCounter == slaveCounter) {
				startReduceJob(request.getJobId());
			}
		}
	}

	/**
	 * start reduce job
	 * @param jobId
	 */
	private void startReduceJob(int jobId) {
		try {
			JobManager jobMaster = jobIdJobManagerMap.get(jobId);
			jobMaster.startReduce();
		} catch(Exception e) {
			LOGGER.error(e.getMessage());
		}
	}

	/**
	 * start partition task
	 * @param jobId
	 */
	private void sendPartitions(int jobId) {
		try {
			JobManager jobMaster = jobIdJobManagerMap.get(jobId);
			jobMaster.sendPartitions();
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			System.exit(0);
		}
	}

	/**
	 * generate pivots for sort
	 * @param jobId
	 * @param samples
	 */
	private void generatePivots(int jobId, List<String> samples) {
		try {
			JobManager jobMaster = jobIdJobManagerMap.get(jobId);
			jobMaster.generatePivots(samples);
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			System.exit(0);
		}
	}

	/**
	 * update status of request after a task is complete
	 * @param request
	 */
	private void updateStatus(NettyRequest request) {
		int jobId = request.getJobId();
		try {
			JobManager jobMaster = jobIdJobManagerMap.get(jobId);
			jobMaster.updateStatus(request);
			if (request.getType().equals(NettyRequestType.PART_MAP_DONE))
				jobMaster.startMapJob();
			if (request.getType().equals(NettyRequestType.PART_SORT_DONE))
				jobMaster.startSort();
			if (request.getType().equals(NettyRequestType.PART_REDUCE_DONE))
				jobMaster.startReduce();
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			// write error file
			System.exit(0);
		}
	}

	/**
	 * start map job
	 * @param jobId
	 * @param request
	 */
	private void startMapJob(int jobId, NettyRequest request) {
		try {
			JobManager jobMaster = JobManagerFactory.getJobManager(jobMode, jobId, request, slaveChannels);
			jobIdJobManagerMap.put(jobId, jobMaster);
			jobMaster.startMapJob();
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			// write error file
			System.exit(0);
		}
	}

	/**
	 * start sort
	 * @param jobId
	 * @param request
	 */
	private void startSort(int jobId, NettyRequest request) {
		try {
			JobManager jobMaster = JobManagerFactory.getJobManager(jobMode, jobId, request, slaveChannels);
			jobIdJobManagerMap.put(jobId, jobMaster);
			jobMaster.startMapJob();
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			// write error file
			System.exit(0);
		}
	}

	/**
	 * register slave channel
	 * @param channel
	 */
	private void registerSlaveChannel(Channel channel) {
		synchronized (MasterChannelhandler.class) {
			slaveChannels.put(++slaveCounter, channel);
			channel.writeAndFlush(getRegistrationAcknowledgeMent(slaveCounter));
		}
	}

	/**
	 * get registration acknowledge after slave registration
	 * @param slaveId
	 * @return
	 */
	private NettyRequest getRegistrationAcknowledgeMent(int slaveId) {
		NettyRequest request = new NettyRequest(NettyRequestType.REGISTRATION_ACK);
		request.setParamValue("SLAVE_ID", slaveId);
		return request;
	}

	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		cause.printStackTrace();
	}
}
