package com.mr.hw.mapred.tasks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;

import com.mr.hw.cluster.FileSystem.FileSystem;
import com.mr.hw.cluster.job.Job;
import com.mr.hw.cluster.job.datastructure.KeyValuePair;
import com.mr.hw.cluster.jobmanager.MRJobManager;
import com.mr.hw.cluster.request.NettyRequest;

/**
 * Sort task for sorting data after map phase
 * @author Sri , mayuri
 *
 */
public class SortTask {

	private int jobId;
	private static NettyRequest request;
	private static int slaveId;
	private Job job;
	private static FileSystem fileSystem;
	private static int slaveCount;

	public SortTask(NettyRequest request, int slaveId, FileSystem fileSystem, Job job, int jobId) throws Exception {
		this.request = request;
		this.jobId = jobId;
		this.slaveId = slaveId;
		this.fileSystem = fileSystem;
		this.job = job;
		this.slaveCount = (Integer) request.getParamValue("slaveCount");
		LOGGER.info("SortTask created at slave : " + slaveId);
	}

	protected static final Logger LOGGER = Logger.getLogger(MRJobManager.class);

	static List<List<KeyValuePair>> chunksToBeSent = new ArrayList<List<KeyValuePair>>();

	/**
	 * Generate pivots for sort
	 * @param samples
	 * @return
	 */
	public static List<String> generatePivots(List<String> samples, int slaveCount) {
		LOGGER.info("Generating pivots from samples");
		Collections.sort(samples);
		List<String> pivots = new ArrayList<String>();
		int r = Math.floorDiv(slaveCount, 2);
		for (int i = 1; i < slaveCount; i++) {
			int index = (slaveCount * i) + r;
			pivots.add(samples.get(index));
		}
		return pivots;
	}

	/**
	 * generate samples for sort
	 * @param dataList
	 * @return
	 */
	public List<String> generateSamples(List<KeyValuePair> dataList) {
		LOGGER.info("Generating samples");
		int w = dataList.size() / slaveCount;
		List<String> samples = new ArrayList<String>();
		for (int i = 0; i < slaveCount; i++) {
			int index = (i * w) + 1;
			samples.add(dataList.get(index - 1).getKey());
		}
		return samples;
	}

	/**
	 * Generate chunks that will be distributed for sort
	 * @param dataList
	 * @return
	 * @throws IOException
	 */
	public static boolean generateChunks(List<KeyValuePair> dataList) throws IOException {
		List<String> pivots = (List<String>) request.getParamValue("pivots");

		LOGGER.info("Partitioning the local data file based on pivots");
		for (int i = 0; i < slaveCount; i++) {
			chunksToBeSent.add(i, new ArrayList<KeyValuePair>());
		}

		if(pivots.size() == 0 && slaveCount ==1) {
			chunksToBeSent.get(0).addAll(dataList);
		} else {
			for (int dataIndex = 0; dataIndex < dataList.size(); dataIndex++) {
				KeyValuePair data = dataList.get(dataIndex);
				for (int pivotIndex = 0; pivotIndex < pivots.size(); pivotIndex++) {
					String key = pivots.get(pivotIndex);
	
					if (pivotIndex == 0 && data.getKey().compareTo(key) <= 0) {
						chunksToBeSent.get(pivotIndex).add(data);
						break;
					}
					if (pivotIndex == pivots.size() - 1 && data.getKey().compareTo(key) == 1) {
						chunksToBeSent.get(pivotIndex + 1).add(data);
						break;
					}
					String nextKey = pivots.get(pivotIndex + 1);
					if (data.getKey().compareTo(key) == 1 && data.getKey().compareTo(nextKey) <= 0) {
						chunksToBeSent.get(pivotIndex + 1).add(data);
						break;
					}
				}
			}
		}
		return sendChunks(slaveId);
	}

	/**
	 * send the data chunks for partition
	 * @param currentSlaveId
	 * @return
	 * @throws IOException
	 */
	private static boolean sendChunks(int currentSlaveId) throws IOException {
		boolean flag = true;
		LOGGER.info("Sending the chunks to sort nodes");
		for (int i = 1; i < chunksToBeSent.size() + 1; i++) {
			String fileName = slaveId + "_" + i;
			fileSystem.writeToTemp("partitions", fileName, chunksToBeSent.get(i - 1));
			if (i != slaveId) {
			//TODO Deal with temp deletion later.
			//	flag = SftpTransferManager.sftpMain(Constants.PARTITION + Constants.SUFFIX + fileName, Constants.PARTITION);
			/*	if (flag)
					fileSystem.deleteFile(new File("partitions/" + fileName));*/
			}
		}
		return flag;
	}
}