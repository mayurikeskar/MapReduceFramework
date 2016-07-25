package com.mr.hw.cluster.FileSystem;

import com.mr.hw.cluster.Utils.Constants;

/**
 * FileSystemFactory that creates a filesytem based on the mode
 * 
 * @author kamlendrak
 */
public class FileSystemFactory {
	
	/**
	 * Returns the File system to be used (either local or aws) depending on mode of execution
	 * @param mode
	 * @return
	 */
	public static FileSystem getFileSystem(String mode) {
		return Constants.AWS.equalsIgnoreCase(mode) ? new AWSFileSystem() : new LocalfileSystem();
	}
	
}
