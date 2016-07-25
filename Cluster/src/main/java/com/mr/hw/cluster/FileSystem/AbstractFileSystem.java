package com.mr.hw.cluster.FileSystem;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.mr.hw.cluster.Utils.Constants;
import com.mr.hw.cluster.Utils.ValueComparator;
import com.mr.hw.cluster.job.datastructure.KeyValuePair;


/**
 * Abstract class that handles generic implementation of FileSystem Operations
 * 
 * @author Mayuri, Kavya, Kamlendra, Sri
 */

public abstract class AbstractFileSystem implements FileSystem {

	private static final Logger LOGGER = Logger.getLogger(AbstractFileSystem.class);

	/**
	 * Sorts the files based on size and distributes it amongst the slaves
	 * @param listOfFiles
	 * @param nodeCount
	 * @return
	 */
	public static List<List<String>> sortByFileSize(HashMap<String, Long> listOfFiles, int nodeCount) {

		Map<String, Long> sortedMap = sortByValue(listOfFiles);
		List<List<String>> fileBalancedList = new ArrayList<List<String>>();
		boolean flag = true;
		int i = 0;
		for (int j = 0; j < nodeCount; j++) {
			List<String> li = new ArrayList<String>();
			fileBalancedList.add(li);
		}
		Set<String> all = sortedMap.keySet();
		Iterator<String> it = all.iterator();
		while (it.hasNext()) {
			fileBalancedList.get(i).add(it.next());
			if (flag) i++;
			else i--;
			if (i == fileBalancedList.size() || i == -1) {
				if (flag) {
					flag = !flag;
					i--;
				} else {
					flag = !flag;
					i++;
				}
			}
		}
		return fileBalancedList;
	}

	/**
	 * Sorts the file name size map by size.
	 * @param fileSizeMap
	 * @return <String>
	 */
	public static Map<String, Long> sortByValue(HashMap<String, Long> fileSizeMap) {
		@SuppressWarnings("unchecked")
		Map<String, Long> sortedMap = new TreeMap<String, Long>(new ValueComparator(fileSizeMap));
		sortedMap.putAll(fileSizeMap);
		return sortedMap;
	}
	
	/**
	 * Retrieves a list of file names in the partitions folder
	 * @param slaveId
	 * @param action
	 * @return List<String>
	 */
	public List<String> getPartitionFiles(int slaveId, String action) throws IOException{		
		List<String> listOfPartitions = new ArrayList<String>();		
		if(action.equals(Constants.PARTITION)){	
			File folder = new File(Constants.PARTITION);
			for(File file : folder.listFiles()){
				if(file.getName().contains(Constants.UNDERSCORE+slaveId))
					listOfPartitions.add(file.getName());
			}
		}
		return listOfPartitions;
	}
	
	/**
	 * Returns a list of key value pairs for each line in the file
	 * @param fileName
	 * @return List<KeyValuePair>
	 */
	public List<KeyValuePair> collectLines(String fileName) throws IOException{
		LOGGER.info("[ Collecting lines from "+fileName+" into a list ]");
		List<KeyValuePair> listOfKeyValue = new ArrayList<KeyValuePair>();
		InputStream input = new FileInputStream(new File(fileName));
		Reader decoder = new InputStreamReader(input);
		BufferedReader br = new BufferedReader(decoder);
		String line;
		KeyValuePair keyValue = null;
		while((line = br.readLine()) != null){
			String[] keyValueArray = line.split(Constants.SEPARATOR);
			keyValue = new KeyValuePair(keyValueArray[Constants.KEY], keyValueArray[Constants.VALUE]);
			listOfKeyValue.add(keyValue);
		}
		br.close();
		return listOfKeyValue;
	}
	
	/**
	 * Writes the data into a file based on the key
	 * @param globalSortData
	 * @return void
	 */
	public void writeToKeyFolder(List<KeyValuePair> globalSortData) throws IOException{
		LOGGER.info("[ Writing the shuffle data based on keys ]");
		File keyFile = new File(globalSortData.get(0).getKey());
		createFolder("", Constants.REDUCER_INPUT);
		FileWriter fw = new FileWriter(Constants.REDUCER_INPUT+keyFile);
		BufferedWriter out = new BufferedWriter(fw);
		out.write(globalSortData.get(0).toString());
		out.newLine();
		for(int i = 1; i<globalSortData.size(); i++){
			if(globalSortData.get(i).getKey().equals(keyFile.getName())){
				out.write(globalSortData.get(i).toString());
				out.newLine();
			}
			else{
				out.close();
				keyFile = new File(globalSortData.get(i).getKey());
				fw = new FileWriter(Constants.REDUCER_INPUT+keyFile);
				out = new BufferedWriter(fw);
				out.write(globalSortData.get(i).toString());
				out.newLine();
			}
		}
		out.close();
	}
	
	/**
	 * Writes the list of key value pairs onto a temporary folder
	 * @param path
	 * @param chunksToBeSent
	 * @return void
	 */
	public void writeToTemp(String folder, String fileName, List<KeyValuePair> chunksToBeSent) throws IOException {
		File file = new File(folder);
		if(!file.exists()) {
			file.mkdir();
		}
		file = new File(folder + Constants.SUFFIX + fileName);
		FileWriter fw = new FileWriter(file);
		BufferedWriter out = new BufferedWriter(fw);
		for(KeyValuePair kv : chunksToBeSent){
			out.write(kv.toString());
			out.newLine();
		}
		out.close();
	}
}