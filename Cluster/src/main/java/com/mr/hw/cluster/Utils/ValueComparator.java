package com.mr.hw.cluster.Utils;

import java.util.Comparator;
import java.util.Map;

/**
 * Compares Map object
 * @author kamlendra, Mayuri, Kavya, Sri Annapurna
 *
 */
public class ValueComparator implements Comparator {
	Map<String, Long> map;

	public ValueComparator(Map<String, Long> map) {
		this.map = map;
	}

	/**
	 * Compare key of two Maps
	 */
	public int compare(Object keyA, Object keyB) {
		Comparable valueA = (Comparable) map.get(keyA);
		Comparable valueB = (Comparable) map.get(keyB);
		return valueB.compareTo(valueA);
	}
}

