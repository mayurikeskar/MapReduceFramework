
package com.mr.hw.mapred.tasks;

import java.io.BufferedReader;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.mr.hw.cluster.FileSystem.FileSystem;
import com.mr.hw.cluster.Utils.Constants;
import com.mr.hw.cluster.job.Job;
import com.mr.hw.cluster.request.NettyRequest;
import com.mr.hw.mapred.Reducer;

/**
 * Class to manage the reduce task.
 * @author kavyashree
 */
public class ReduceTask {

	private static Logger LOGGER = Logger.getLogger(ReduceTask.class);

	private int jobId;
	private int slaveId;
	private int taskId;
	private Class<? extends Reducer> userReducerClass;
	private Object userReducerObject;
	private Job job;
	private String fileForReducer;
	private FileSystem fileSystem;

	/**
	 * Constructor
	 * 
	 * @param request
	 *            - has job information
	 * @param slaveId
	 * @param fileSystem
	 *            - Local or aws depending on the mode of execution
	 * @throws Exception
	 */
	public ReduceTask(NettyRequest request, int slaveId, FileSystem fileSystem) throws Exception {
		this.jobId = request.getJobId();
		this.slaveId = slaveId;
		this.fileSystem = fileSystem;
		this.taskId = (Integer) request.getParamValue("taskId");
		this.job = (Job) request.getParamValue("JOB");
		this.fileForReducer = (String) request.getParamValue("file");
		this.userReducerClass = getUserReducerClass();
		this.userReducerObject = this.userReducerClass.newInstance();
		LOGGER.info("ReduceTask created at slave : " + slaveId + "for file : " + request.getParamValue("file"));
	}

	/**
	 * Get reducer class from jar
	 * 
	 * @return
	 * @throws Exception
	 */
	private Class<? extends Reducer> getUserReducerClass() throws Exception {
		Class<? extends Reducer> m = (Class<? extends Reducer>) this.getClass().getClassLoader()
				.loadClass(job.getReducerClass());
		return m;
	}

	/**
	 * Reads the file and converts the data into <Key , Iterable<Value>> . Call
	 * reduce for eack key
	 * 
	 * @throws Exception
	 */
	public void start() throws Exception {
		Method reduce = this.userReducerClass.getMethod("reduce", new Class<?>[] { MyTaskContext.class, String.class , 
			Iterable.class});
		HashMap<String, Iterable<String>> keyValues = new HashMap<String, Iterable<String>>();
		try {
			BufferedReader br = fileSystem.readFile(Constants.REDUCER_INPUT, fileForReducer);
			String str;
			while ((str = br.readLine()) != null) {
				String key = str.split(Constants.SEPARATOR)[Constants.ZERO];
				Iterable<String> values = new ArrayList<String>();

				if (!keyValues.containsKey(key)) {
					((ArrayList<String>) values).add(str.split(Constants.SEPARATOR)[1]);
					keyValues.put(key, values);
				} else {
					values = keyValues.get(key);
					((ArrayList<String>) values).add(str.split(Constants.SEPARATOR)[1]);
					keyValues.put(key, values);
				}
			}
			br.close();
		} catch (IOException e) {
			LOGGER.info("Reduce task exception : " + jobId + " at slaveId : " + slaveId);
		}
		MyTaskContext myTaskContext = new MyTaskContext(this.job.getConf());

		for (Entry<String, Iterable<String>> keyValue : keyValues.entrySet()) {
			Iterable<String> values = keyValue.getValue();
			String key = keyValue.getKey();
			reduce.invoke(userReducerObject, new Object[] {myTaskContext, key, values});
		}
		this.fileSystem.writeToTemp("reduce", jobId + "-" + slaveId + "-" + taskId, myTaskContext.getOutputCollector());
		
	}
}