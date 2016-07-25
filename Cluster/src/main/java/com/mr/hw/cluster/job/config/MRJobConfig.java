/**
 * 
 */
package com.mr.hw.cluster.job.config;

/**
 * Class that holds required information for mapreduce job
 * @author kamlendrak
 *
 */
public class MRJobConfig {

	public static final String JAR_NAME = "jar.name";
	public static final String JOB_NAME = "job.name";
	public static final String DRIVER_CLASS = "job.driver";
	public static final String MAPPER_CLASS = "job.mapper";
	public static final String REDUCER_CLASS = "job.reducer";
	public static final String OUTPUT_KEY_CLASS = "job.output.key.class";
	public static final String OUTPUT_VALUE_CLASS = "job.output.value.class";
	public static final String INPUT_PATH = "job.input.path";
	public static final String OUTPUT_PATH = "job.output.path";

}
