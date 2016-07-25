
package com.mr.hw.cluster.slave;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.mr.hw.cluster.request.NettyRequest;
import com.mr.hw.cluster.request.NettyRequestType;
import com.mr.hw.cluster.taskmanager.TaskManager;
import com.mr.hw.cluster.taskmanager.TaskManagerFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * Slave request handler
 * @author kamlendra
 *
 */
public class SlaveChannelhandler extends ChannelInboundHandlerAdapter {

	private static final Logger LOGGER = Logger.getLogger(SlaveChannelhandler.class);

	private int slaveId;
	private Channel masterChannel;
	private static String MODE;
	private Map<Integer, TaskManager> jobTaskManagerMap;

	public SlaveChannelhandler(Channel masterChannel, String mode) {
		this.masterChannel = masterChannel;
		if (MODE == null || mode.length() == 0) {
			MODE = mode;
		}
		jobTaskManagerMap = new HashMap<Integer, TaskManager>();
	}

	@Override
	public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
		System.out.println("channelRegistered");
	}

	@Override
	public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
		System.out.println("channelUnregistered");
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		System.out.println("channelActive");
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		System.out.println("channelInactive");
	}

	/**
	 * call appropriate task based on the request type
	 */
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		NettyRequest request = (NettyRequest) msg;
		LOGGER.info(request.getType() + " : " + request.getCalledId());
		
		if (request.getType() == NettyRequestType.REGISTRATION_ACK) {
			assignSlaveId(request);
		}

		if (request.getType() == NettyRequestType.START_MAP) {
			TaskManager taskManager = getTaskManagerForJob(request);
			taskManager.startMapTask(request);
		}
		
		if (request.getType() == NettyRequestType.START_REDUCE) {
			TaskManager taskManager = getTaskManagerForJob(request);
			taskManager.startReduceTask(request);
		}
		
		if (request.getType() == NettyRequestType.START_SORT) {
			startSort(request);
		}
		
		if (request.getType() == NettyRequestType.PHASE1) {
			generateSamples(request);
		}
		
		if (request.getType() == NettyRequestType.PHASE2) {
			startPhase2(request);
		}
		
		if (request.getType() == NettyRequestType.PHASE3) {
			startPhase3(request);
		}
	}

	/**
	 * sort data on slave 
	 * @param request
	 * @throws Exception
	 */
	private void startPhase3(NettyRequest request) throws Exception {
		TaskManager taskManager = jobTaskManagerMap.get(request.getJobId());
		taskManager.startPhase3((List<String>)request.getParamValue("partitionList"));
	}
	
	/**
	 * Slave waiting for pivot
	 * @param request
	 * @throws Exception
	 */
	private void startPhase2(NettyRequest request) throws Exception {
		TaskManager taskManager = jobTaskManagerMap.get(request.getJobId());
		taskManager.startPhase2(request);
	}
	
	/**
	 * generate samples for sort
	 * @param request
	 * @throws Exception
	 */
	private void generateSamples(NettyRequest request) throws Exception {
		TaskManager taskManager = jobTaskManagerMap.get(request.getJobId());
		taskManager.generateSamples(request);
	}
	
	/**
	 * start sort task
	 * @param request
	 * @throws IOException
	 */
	private void startSort(NettyRequest request) throws IOException{
		TaskManager taskManager = jobTaskManagerMap.get(request.getJobId());
		taskManager.startSort(request);
	}
	
	
	/**
	 * Creates a new TaskManager if it is first request for a job. Otherwise
	 * returns the old task manager for this job.
	 * 
	 * @param request
	 * @return
	 * @throws Exception
	 */
	private TaskManager getTaskManagerForJob(NettyRequest request) throws Exception {
		int jobId = request.getJobId();
		TaskManager taskManager = jobTaskManagerMap.get(jobId);
		if (taskManager == null) {
			synchronized (this) {
				taskManager = TaskManagerFactory.getTaskManager(MODE, jobId, slaveId, masterChannel, request);
				jobTaskManagerMap.put(jobId, taskManager);
			}
		}
		return taskManager;
	}

	/**
	 * set slave id
	 * @param request
	 */
	private void assignSlaveId(NettyRequest request) {
		this.slaveId = ((Integer) request.getParamValue("SLAVE_ID")).intValue();
		System.out.println("Slave id assigned : " + this.slaveId);
	}

	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		cause.printStackTrace();
		cause.printStackTrace();
	}
}
