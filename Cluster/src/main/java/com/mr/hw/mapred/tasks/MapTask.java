package com.mr.hw.mapred.tasks;

import java.io.BufferedReader;
import java.lang.reflect.Method;

import org.apache.log4j.Logger;

import com.mr.hw.cluster.FileSystem.FileSystem;
import com.mr.hw.cluster.job.Job;
import com.mr.hw.cluster.request.NettyRequest;
import com.mr.hw.mapred.Mapper;

/**
 * Class to start the map job.
 * @author kamlendrak
 *
 */
public class MapTask {

	private static Logger LOGGER = Logger.getLogger(MapTask.class);

	private int taskId;
	private int jobId;
	private int slaveId;
	private Class<? extends Mapper> mapperClass;
	private Object userMapperObject;
	private Job job;
	private String fileForMapper;
	private FileSystem fileSystem;

	public MapTask(NettyRequest request, int slaveId, FileSystem fileSystem, Job job, int jobId) throws Exception {
		this.jobId = jobId;
		this.slaveId = slaveId;
		this.fileSystem = fileSystem;
		this.job = job;
		this.taskId = (Integer) request.getParamValue("taskId");
		this.fileForMapper = (String) request.getParamValue("file");
		this.mapperClass = getUserMapperClass();
		this.userMapperObject = this.mapperClass.newInstance();
		LOGGER.info("MapTask created at slave : " + slaveId + "for file : " + request.getParamValue("file"));
	}

	/**
	 * Loads the user's Mapper class using reflection.
	 * @param request
	 * @return
	 * @throws Exception
	 */
	private Class<? extends Mapper> getUserMapperClass() throws Exception {
		Class<? extends Mapper> m = (Class<? extends Mapper>) this.getClass().getClassLoader()
				.loadClass(job.getMapperClass());
		return m;
	}

	/**
	 * Starts the map job.
	 * @throws Exception
	 */
	public void start() throws Exception {
		MyTaskContext myTaskContext = new MyTaskContext(this.job.getConf());
		Method map = this.mapperClass.getMethod("map", new Class<?>[] { MyTaskContext.class, String.class });
		BufferedReader reader = fileSystem.readFile(job.getInputPath(), fileForMapper);
		String line = null;
		while ((line = reader.readLine()) != null) {
			map.invoke(userMapperObject, new Object[] { myTaskContext, line });
		}
		this.fileSystem.writeToTemp("map", +jobId + "-" + slaveId + "-" + taskId, myTaskContext.getOutputCollector());
	}

	// TODO Remove the below code once reflection is working.
	/*
	 * public void start() throws Exception { MyTaskContext myTaskContext = new
	 * MyTaskContext(this.job.getConf()); MyMapper mapper = new MyMapper();
	 * BufferedReader reader = fileSystem.readFile(job.getInputPath(),
	 * fileForMapper); String line = null; while ((line = reader.readLine()) !=
	 * null) { mapper.map(myTaskContext, line); }
	 * this.fileSystem.writeToTemp("map", jobId + "-" + slaveId + "-" + taskId,
	 * myTaskContext.getOutputCollector()); }
	 */
}