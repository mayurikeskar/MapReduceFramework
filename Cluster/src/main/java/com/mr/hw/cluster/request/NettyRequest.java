/**
 * 
 */
package com.mr.hw.cluster.request;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Object passed on the network 
 * @author kamlendrak
 *
 */
public class NettyRequest implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private NettyRequestType type;
	
	private int jobId;
	private int calledId;
	
	private Map<String, Object> params;
	
	public NettyRequest(NettyRequestType type) {
		this.type = type;
		params = new HashMap<String, Object>();
	}

	/**
	 * @return the type
	 */
	public NettyRequestType getType() {
		return type;
	}

	/**
	 * @param set the type
	 */
	public void setType(NettyRequestType type) {
		this.type = type;
	}

	/**
	 * @return the jobId
	 */
	public int getJobId() {
		return jobId;
	}

	/**
	 * @param the jobId to set
	 */
	public void setJobId(int jobId) {
		this.jobId = jobId;
	}

	/**
	 * @return the calledId
	 */
	public int getCalledId() {
		return calledId;
	}

	/**
	 * @param the calledId to set
	 */
	public void setCalledId(int calledId) {
		this.calledId = calledId;
	}
	
	/**
	 * 
	 * @param get key
	 * @return
	 */
	public Object getParamValue(String key) {
		return params.get(key);
	}
	
	/**
	 * 
	 * @param set key
	 * @param value
	 */
	public void setParamValue(String key, Object value) {
		this.params.put(key, value);
	}
}