package com.mr.hw.cluster.FileSystem;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.mr.hw.cluster.job.datastructure.KeyValuePair;
import com.mr.hw.mapred.job.status.FileOperationStatus;


/**
 * FileSystem interface that is used to provide an abstraction for the local and AWS system
 * 
 * @author Mayuri, Sri, Kavya and Kamlendra
 */	
public interface FileSystem {
	
	/**
	 * Checks the size of files in the folder and divides them by size.
	 * @param path
	 * @param sortNodeCount
	 * @return
	 * @throws IOException
	 */
	public Map<String, FileOperationStatus> accessInput(String path) throws IOException;
	
	/**
	 * Creates a folder in the specified path
	 * @param path
	 * @throws IOException
	 */
	public void createFolder(String path, String name) throws IOException;
	
	/**
	 * Deletes the folder 
	 * @param folder
	 * @return 
	 * @throws IOException
	 */
	public void deleteFolder(String path, String folder) throws IOException;
	
	/**
	 * Creates file in the given folder
	 * @throws IOException
	 */
	public void createFile(File folder) throws IOException;
	
	/**
	 * Deletes a file 
	 * @param file
	 * @return 
	 * @throws IOException
	 */
	public void deleteFile(File file) throws IOException;

	/**
	 * gets the jar from the path
	 * @param path
	 * @throws IOException
	 */
	public File getJar(String bucketname, String jobName) throws IOException;
	
	/**
	 * Reads the data from all the given files.
	 * @param listOfFiles
	 * @return
	 * @throws IOException
	 */
	public BufferedReader readFile(String path, String key) throws IOException;	
	
	/**
	 * Writes the result to the specified file.
	 * @param TemperatureObjects
	 * @param sortNode
	 * @param outputPath
	 * @throws IOException
	 */
	public void writeToFile(String path, String key) throws IOException;
	
	
	/**
	 * Checks if output folder exists in the given path/ bucket name
	 * @param path
	 * @param key
	 * @throws Exception
	 */
	public void checkOutputFolder(String path, String key) throws Exception;
	
	/**
	 * Checks if input folder exists.
	 * @param path
	 * @param key
	 * @throws Exception
	 */
	public void checkInputFolder(String path, String key) throws Exception;
	
	/**
	 * Gets the partition files for the designated slave based on action
	 * @param slaveId
	 * @param action
	 * @return
	 * @throws IOException
	 */
	public List<String> getPartitionFiles(int slaveId, String action) throws IOException;
	
	/**
	 * Collects the lines into a list of key value pairs based on fileName
	 * @param fileName
	 * @return
	 * @throws IOException
	 */
	public List<KeyValuePair> collectLines(String fileName) throws IOException;
	
	/**
	 * Writes the list of key value pairs into the key folder 
	 * @param globalSortData
	 * @throws IOException
	 */
	public void writeToKeyFolder(List<KeyValuePair> globalSortData) throws IOException;
	
	/**
	 * Writes the partitions in the given path
	 * @param path
	 * @param chunksToBeSent
	 * @throws IOException
	 */
	public void writeToTemp(String folder, String fileName, List<KeyValuePair> chunksToBeSent) throws IOException;
}
