package com.mr.hw.cluster.FileSystem;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.GZIPInputStream;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.IOUtils;
import com.mr.hw.cluster.Utils.Constants;
import com.mr.hw.mapred.job.status.FileOperationStatus;

/**
 * Class that handles AWS specific File System operations
 * 
 * @author Mayuri, Sri, Kavya, Kamlendra
 */
public class AWSFileSystem extends AbstractFileSystem {
	static HashMap<String, String> mapOfKeyValue = new HashMap<String, String>();
	AmazonS3 s3;
	

	/**
	 * Sets up the credentials.
	 * 
	 * @param accessKey
	 * @param secretKey
	 */
	public AWSFileSystem() {
		s3 = new AmazonS3Client(new ProfileCredentialsProvider());
	}

	/**
	 * Splits the file list in bucket by size.
	 * 
	 * @param bucketName
	 * @param sortNodeCount
	 * @return
	 * @throws IOException
	 */
	public Map<String, FileOperationStatus> getFileSizeFromS3(String bucketName) throws IOException {
		Map<String, FileOperationStatus> listOfFiles = new HashMap<String, FileOperationStatus>();
		ObjectListing objects = s3.listObjects(bucketName);
		do {
			for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
				if (objectSummary.getKey().startsWith(Constants.INPUT) && objectSummary.getSize() > 0) {
					listOfFiles.put(objectSummary.getKey(), FileOperationStatus.NOT_STARTED);
					System.out.println(bucketName + Constants.SUFFIX + objectSummary.getKey() + " " + objectSummary.getSize());
				}
			}
			objects = s3.listNextBatchOfObjects(objects);
		} while (objects.isTruncated());
		return listOfFiles;
	}

	/**
	 * Reads the data from given bucket.
	 * 
	 * @param input
	 * @return
	 * @throws IOException
	 */
	public List<String> getDataFromS3(String input) throws IOException {
		String inputFile[] = input.split(Constants.SUFFIX);
		S3Object s3object = s3.getObject(new GetObjectRequest(inputFile[0], inputFile[1] + Constants.SUFFIX + inputFile[2]));
		System.out.println("Content-Type: " + s3object.getObjectMetadata().getContentType());
		@SuppressWarnings("unused")
		InputStream fileIn = new GZIPInputStream(s3object.getObjectContent());
		return null;
	}

	/**
	 * Gets input files names from the input bucket name
	 */
	public Map<String, FileOperationStatus> accessInput(String bucketName) throws IOException {
		return getFileSizeFromS3(bucketName);
	}

	/**
	 * Read each file and convert it into objects
	 * @param listOfFiles
	 * @return
	 */
	public List<String> readFile(List<String> listOfFiles) {
		List<String> listOfClimate = new ArrayList<String>();
		for (int i = 0; i < listOfFiles.size(); i++) {
			try {
				listOfClimate.addAll(getDataFromS3(listOfFiles.get(i)));
			} catch (IOException e) {
			}
		}

		return listOfClimate;
	}

	/**
	 * @param key
	 * @param value
	 * @throws IOException
	 */
	public void writeToMap(String key, String value) throws IOException {
		mapOfKeyValue.put(key, value);
	}

	/**
	 * Writes to S3 bucket based on given key
	 * @param bucketName
	 * @param key
	 * @return void
	 */
	public void writeToFile(String bucketName, String key) throws IOException {
		String fileName = Constants.PART_OUTPUT + key;
		File file = new File(bucketName + Constants.SUFFIX + key);
		if (file.exists()) {
			file.delete();
			createFolder(bucketName, key);
		}
		FileWriter fw = new FileWriter(bucketName + Constants.SUFFIX + key);
		BufferedWriter out = new BufferedWriter(fw);
		Iterator<Entry<String, String>> it = mapOfKeyValue.entrySet().iterator();
		while (it.hasNext()) {
			out.write(it.next().getKey() + Constants.SEPARATOR + it.next().getValue());
			out.newLine();
		}
		out.close();

		s3 = new AmazonS3Client(new ProfileCredentialsProvider());
		try {
			System.out.println("Uploading a new object to S3 from a file\n");
			s3.putObject(new PutObjectRequest(bucketName, fileName, file));
		} catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which " + "means your request made it "
					+ "to Amazon S3, but was rejected with an error response" + " for some reason.");
		}
	}

	/**
	 * Create folder in the bucket
	 */
	public void createFolder(String bucketName, String folderName) throws IOException {
		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentLength(0);
		InputStream emptyContent = new ByteArrayInputStream(new byte[0]);
		PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, folderName + Constants.SUFFIX,
				emptyContent, metadata);
		s3.putObject(putObjectRequest);
	}

	/**
	 * Delete folder from the bucket
	 */
	public void deleteFolder(String bucketName, String folder) throws IOException {
		List<S3ObjectSummary> fileList = s3.listObjects(bucketName, folder).getObjectSummaries();
		for (S3ObjectSummary file : fileList) {
			s3.deleteObject(bucketName, file.getKey());
		}
		s3.deleteObject(bucketName, folder);
	}

	/**
	 * Get job jar
	 */
	public File getJar(String bucketName, String jobName) throws IOException {
		s3.getObject(new GetObjectRequest(bucketName, jobName), new File(jobName));
		return new File(jobName);
	}

	public void createFile(File folder) throws IOException {

	}

	public void deleteFile(File file) throws IOException {

	}

	/**
	 * Read file from bucket
	 */
	public BufferedReader readFile(String bucketName, String fileName) throws IOException {
		S3Object s3object = s3.getObject(new GetObjectRequest(bucketName, fileName));
		System.out.println("Content-Type: " + s3object.getObjectMetadata().getContentType());
		InputStream fileIn = null;
		if (s3object.toString().contains(Constants.GZIP_EXT)) {
			fileIn = new GZIPInputStream(s3object.getObjectContent());
		} else {
			fileIn = s3object.getObjectContent();
		}
		return new BufferedReader(new InputStreamReader(fileIn));
	}

	/**
	 * Download file from bucket
	 * @param bucketName
	 * @param key
	 * @throws IOException
	 */
	public void downloadFile(String bucketName, String key) throws IOException {
		GetObjectRequest request = new GetObjectRequest(bucketName, key);
		S3Object object = s3.getObject(request);
		S3ObjectInputStream objectContent = object.getObjectContent();
		IOUtils.copy(objectContent, new FileOutputStream(Constants.DRIVE + key));
	}

	/**
	 * Check if output folder exists in bucket
	 */
	public void checkOutputFolder(String bucketName, String key) throws Exception {
		ObjectListing objects = s3.listObjects(bucketName);
		S3ObjectSummary object = new S3ObjectSummary();
		object.setKey(key);
		if (objects.getObjectSummaries().contains(object))
			throw new Exception(key + "output folder already exists");
	}

	/**
	 * Check if input folder does not exists in bucket
	 */
	public void checkInputFolder(String bucketName, String key) throws Exception {
		ObjectListing objects = s3.listObjects(bucketName);
		S3ObjectSummary object = new S3ObjectSummary();
		object.setKey(key);
		if (!objects.getObjectSummaries().contains(object))
			throw new Exception(key + "input folder doesnot exists");
	}


}