package com.mr.hw.cluster.job.datastructure;

import com.mr.hw.cluster.Utils.Constants;

/**
 * Class for key value pair
 * @author Mayuri, Kavya, Sri , kamlendra
 *
 */
public class KeyValuePair implements Comparable<KeyValuePair>{
	
	private String key;
	private String value;
	
	public KeyValuePair(){
		
	}
	
	public KeyValuePair(String key, String value){
		this.key =key;
		this.value = value;
	}

	/**
	 * get key 
	 * @return
	 */
	public String getKey() {
		return key;
	}

	/**
	 * set key
	 * @param key
	 */
	public void setKey(String key) {
		this.key = key;
	}

	/**
	 * get value
	 * @return
	 */
	public String getValue() {
		return value;
	}

	/**
	 * set value
	 * @param value
	 */
	public void setValue(String value) {
		this.value = value;
	}

	/**
	 * compare key
	 */
	public int compareTo(KeyValuePair kv) {
		return this.key.compareTo(kv.getKey());
	}
	
	@Override
	public String toString(){
		return this.key + Constants.SEPARATOR + this.value;
	}
 
}
