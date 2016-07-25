package com.mr.hw.cluster.FileSystem;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.GZIPInputStream;

import org.apache.log4j.Logger;

import com.mr.hw.cluster.Utils.Constants;
import com.mr.hw.mapred.job.status.FileOperationStatus;

/**
 * Class that handles the local file system operations
 * 
 * @author Mayuri, Sri and Kavya
 */
public class LocalfileSystem extends AbstractFileSystem {

	private static final Logger LOGGER = Logger.getLogger(LocalfileSystem.class);
	static HashMap<String, String> mapOfKeyValue = new HashMap<String, String>();

	/**
	 * Checks the size of files in the folder and divides them by size.
	 * 
	 * @param path
	 * @param sortNodeCount
	 * @return
	 * @throws IOException
	 */
	public Map<String, FileOperationStatus> accessInput(String path) throws IOException {
		File folder = new File(path);
		Map<String, FileOperationStatus> filesList = new HashMap<String, FileOperationStatus>();
		LOGGER.info("Retrieving files from the path: " + path);
		for (final File fileEntry : folder.listFiles()) {
			filesList.put(fileEntry.getName(), FileOperationStatus.NOT_STARTED);
		}
		return filesList;
	}

	/**
	 * Writes to key file given a path
	 * 
	 * @param path
	 * @param key
	 * @return void
	 */
	public void writeToFile(String path, String key) throws IOException {
		File file = new File(path + Constants.SUFFIX + key);
		if (file.exists()) {
			file.delete();
			createFolder(path, key);
		}
		FileWriter fw = new FileWriter(path + Constants.SUFFIX + key);
		BufferedWriter out = new BufferedWriter(fw);
		Iterator<Entry<String, String>> it = mapOfKeyValue.entrySet().iterator();
		while (it.hasNext()) {
			out.write(it.next().getKey() + Constants.SEPARATOR + it.next().getValue());
			out.newLine();
		}
		out.close();
	}

	/**
	 * Creates a file in the given folder
	 * 
	 * @param folder
	 * @return void
	 */
	public void createFile(File folder) throws IOException {
		folder.createNewFile();
	}

	/**
	 * Deletes the folder
	 * 
	 * @param path
	 * @param key
	 * @return void
	 */
	public void deleteFolder(String path, String key) throws IOException {
		File file = new File(path + Constants.SUFFIX + key);
		delete(file);
	}

	/**
	 * Deletes the folder.
	 * @param folder
	 * @throws IOException
	 */
	public void delete(File folder) throws IOException {
		if (folder.list().length == 0) {
			folder.delete();
			LOGGER.info("Directory is deleted : " + folder.getAbsolutePath());
		} else {
			String files[] = folder.list();
			for (String temp : files) {
				File fileDelete = new File(folder, temp);
				delete(fileDelete);
			}
			if (folder.list().length == 0) {
				folder.delete();
				LOGGER.info("Directory is deleted : " + folder.getAbsolutePath());
			}
		}
	}

	/**
	 * Creates the folder given a path and name
	 * 
	 * @param path
	 * @param keyName
	 */
	public void createFolder(String path, String keyName) throws IOException {
		File file = new File(keyName);
		if (!file.exists())
			if (file.mkdir())
				LOGGER.info("Directory is created!");
			else
				LOGGER.info("Failed to create directory!");
	}

	/**
	 * Deletes the specified file
	 * 
	 * @param file
	 * @return void
	 */
	public void deleteFile(File file) throws IOException {
		if (file.exists())
			file.delete();
	}

	/**
	 * Retrieves the specified jar
	 * 
	 * @param path
	 * @param jobName
	 * @return File : jar
	 * @throws IOException
	 */
	public File getJar(String path, String jobName) throws IOException {
		File file = new File(path + Constants.SUFFIX + jobName);
		return file;
	}

	/**
	 * Reads a file
	 * 
	 * @param listOfFiles
	 * @return
	 * @throws IOException
	 */
	public BufferedReader readFile(String path, String key) throws IOException {
		if (key.contains(".gz")) {
			GZIPInputStream gzip = new GZIPInputStream(new FileInputStream(new File(path + Constants.SUFFIX + key)));
			Reader decoder = new InputStreamReader(gzip);
			BufferedReader br = new BufferedReader(decoder);
			return br;
		} else {
			FileInputStream inputStream = new FileInputStream(new File(path + Constants.SUFFIX + key));
			Reader decoder = new InputStreamReader(inputStream);
			BufferedReader br = new BufferedReader(decoder);
			return br;
		}
	}

	/**
	 * Checks if output folder exists
	 * 
	 * @param path
	 * @param key
	 * @throws IOException
	 */
	public void checkOutputFolder(String path, String key) throws Exception {
		File folder = new File(path + Constants.SUFFIX + key);
		if (folder.exists())
			throw new Exception(key + " output folder already exists");
	}

	/**
	 * Checks if the input folder does not exist
	 * 
	 * @param path
	 * @param key
	 * @return void
	 * @throws Exception
	 */
	public void checkInputFolder(String path, String key) throws Exception {
		File folder = new File(path + Constants.SUFFIX + key);
		if (!folder.exists())
			throw new Exception(key + " input folder does not exists");
	}
}