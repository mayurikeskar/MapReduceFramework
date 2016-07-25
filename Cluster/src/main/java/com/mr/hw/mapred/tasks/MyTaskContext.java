
package com.mr.hw.mapred.tasks;

import java.util.ArrayList;
import java.util.List;

import com.mr.hw.cluster.job.Configuration;
import com.mr.hw.cluster.job.datastructure.KeyValuePair;

/**
 * Context class for map and reduce job.
 * @author kamlendrak
 *
 */
public class MyTaskContext {
	
	private Configuration config;
	private List<KeyValuePair> outputCollector;
	
	public MyTaskContext(Configuration config) {
		this.config = config;
		this.outputCollector = new ArrayList<KeyValuePair>();
	}

	public void write(String key, String value) {
		this.outputCollector.add(new KeyValuePair(key, value));
	}

	/**
	 * @return the outputCollector
	 */
	public List<KeyValuePair> getOutputCollector() {
		return outputCollector;
	}

	/**
	 * @return the config
	 */
	public Configuration getConfig() {
		return config;
	}
}
