package com.mr.hw.mapred.job.status;

/**
 * Enum that represents the current status of Job 
 * @author kamlendrak
 *
 */

public enum JobStatus {

	RUNNING_MAP,
	MAP_FAILED,
	MAP_COMPLETED,
	RUNNING_REDUCE,
	REDUCE_FAILED,
	REDUCE_COMPLETED,
	RUNNING_SORT,
	SEND_SORT_COMPLETED,
	PHASE1,
	PHASE2,
	PHASE3,
}
