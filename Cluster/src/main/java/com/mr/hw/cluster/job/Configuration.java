/**
 * 
 */
package com.mr.hw.cluster.job;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Configuration class that holds parameters in the form of key value pairs
 * 
 * @author kamlendrak
 *
 */
public class Configuration implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private Map<String, String> params;
	
	/**
	 * Constructor
	 */
	public Configuration() {
		this.params = new HashMap<String, String>();
	}
	
	/**
	 * set property in configuration
	 * @param property
	 * @param value
	 */
	public void set(String property, String value) {
		this.params.put(property, value);
	}
	
	/**
	 * get property from configuration
	 * @param property
	 * @return
	 */
	public String get(String property) {
		return this.params.get(property);
	}
}
