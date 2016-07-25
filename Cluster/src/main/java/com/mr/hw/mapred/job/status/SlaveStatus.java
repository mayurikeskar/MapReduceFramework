/**
 * 
 */
package com.mr.hw.mapred.job.status;

/**
 * Enum that represents the status of slave 
 * @author kamlendrak
 *
 */

public enum SlaveStatus {
	
	IDLE,
	MAP_RUNNING,
	MAP_FAILED,
	MAP_DONE,
	REDUCE_RUNNING,
	REDUCE_FAILED,
	REDUCE_DONE,
	PART_MAP_DONE,
	PART_SORT_DONE,
	SORT_RUNNING,
	SEND_SORT_DONE,
	PHASE1,
	PHASE2,
	PHASE3,
	PART_REDUCE_DONE

}
