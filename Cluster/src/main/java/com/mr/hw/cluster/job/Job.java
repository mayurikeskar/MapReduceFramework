/**
 * 
 */
package com.mr.hw.cluster.job;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.Serializable;

import com.mr.hw.cluster.job.config.MRJobConfig;
import com.mr.hw.cluster.request.NettyRequest;
import com.mr.hw.cluster.request.NettyRequestType;
import com.mr.hw.mapred.Mapper;
import com.mr.hw.mapred.Reducer;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;

/**
 * @author kamlendrak
 * Job class which holds necessary details for mapreduce task
 */
public class Job implements Serializable {

	private static final long serialVersionUID = 1L;

	private Configuration conf;

	/**
	 * set configuration
	 * @param conf
	 */
	public Job(Configuration conf) {
		this.conf = conf;
	}

	/**
	 * create new config for the job
	 */
	public Job() {
		this.conf = new Configuration();
	}
	
	/**
	 * get job config in job config
	 * @return
	 */
	public Configuration getConf() {
		return this.conf;
	}

	/**
	 * set jar name in job config
	 * @param jarName
	 */
	public void setJarName(String jarName) {
		conf.set(MRJobConfig.JAR_NAME, jarName);
	}
	
	/**
	 * get jar name in job config
	 * @return
	 */
	public String getJarName() {
		return conf.get(MRJobConfig.JAR_NAME);
	}

	/**
	 * set job name in job config
	 * @param jobName
	 */
	public void setJobName(String jobName) {
		conf.set(MRJobConfig.JOB_NAME, jobName);
	}
	
	/**
	 * get job name in job config
	 * @return
	 */
	public String getJobName() {
		return conf.get(MRJobConfig.JOB_NAME);
	}

	/**
	 * set driver class in job config
	 * @param classs
	 */
	public void setDriverClass(Class<?> classs) {
		conf.set(MRJobConfig.DRIVER_CLASS, classs.getName());
	}
	
	/**
	 * get driver class in job config
	 * @return
	 */
	public String getDriverClass() {
		return conf.get(MRJobConfig.DRIVER_CLASS);
	}

	/**
	 * set mapper class in job config
	 * @param classs
	 */
	public void setMapperClass(Class<? extends Mapper> classs) {
		conf.set(MRJobConfig.MAPPER_CLASS, classs.getName());
	}
	
	/**
	 * get mapper class in job config
	 * @return
	 */
	public String getMapperClass() {
		return conf.get(MRJobConfig.MAPPER_CLASS);
	}
	
	/**
	 * set reducer class in job config
	 * @param classs
	 */
	public void setReducerClass(Class<? extends Reducer> classs) {
		conf.set(MRJobConfig.REDUCER_CLASS, classs.getName());
	}
	
	/**
	 * get reducer class in job config
	 * @return
	 */
	public String getReducerClass() {
		return conf.get(MRJobConfig.REDUCER_CLASS);
	}

	/**
	 * set output key type in job config
	 * @param classs
	 */
	public void setOutputKeyClass(Class<?> classs) {
		conf.set(MRJobConfig.OUTPUT_KEY_CLASS, classs.getName());
	}
	
	/**
	 * get output key type in job config
	 * @return
	 */
	public String getOutputKeyClass() {
		return conf.get(MRJobConfig.OUTPUT_KEY_CLASS);
	}

	/**
	 * set output value type in job config
	 */
	public void setOutputValueClass(Class<?> classs) {
		conf.set(MRJobConfig.OUTPUT_VALUE_CLASS, classs.getName());
	}
	
	/**
	 * get output value type in job config
	 * @return
	 */
	public String getOutputValueClass() {
		return conf.get(MRJobConfig.OUTPUT_VALUE_CLASS);
	}

	/**
	 * set input path in job config
	 * @param inputPath
	 */
	public void setInputPath(String inputPath) {
		conf.set(MRJobConfig.INPUT_PATH, inputPath);
	}
	
	/**
	 * get input path in job config
	 * @return
	 */
	public String getInputPath() {
		return conf.get(MRJobConfig.INPUT_PATH);
	}

	/**
	 * set output path in job config
	 * @param outputPath
	 */
	public void setOutputPath(String outputPath) {
		conf.set(MRJobConfig.OUTPUT_PATH, outputPath);
	}
	
	/**
	 * get output path in job config
	 * @return
	 */
	public String getOuputPath() {
		return conf.get(MRJobConfig.OUTPUT_PATH);
	}

	/**
	 * submit job to master 
	 * @throws Exception
	 */
	public void submitJob() throws Exception {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(new File("masterInstance.txt")));
			String line = null, masterIp =null;
			while((line = br.readLine())!=null){
				masterIp = line;
			}
			EventLoopGroup worker = new NioEventLoopGroup();
			Bootstrap b = new Bootstrap().group(worker).channel(NioSocketChannel.class)
					.option(ChannelOption.SO_KEEPALIVE, true).handler(new ChannelInitializer<Channel>() {
	
						@Override
						protected void initChannel(Channel ch) throws Exception {
							ChannelPipeline pipeline = ch.pipeline();
							pipeline.addLast(new ObjectDecoder(
									ClassResolvers.weakCachingResolver(NettyRequest.class.getClassLoader())));
							pipeline.addLast(new ObjectEncoder());
						}
					});
			Channel ch = b.connect(masterIp, 8000).sync().channel();
			ch.writeAndFlush(getJobSubmissionRequest());
		} finally {
			br.close();
		}
	}

	/**
	 * get Netty Job submission request 
	 * @return
	 */
	private NettyRequest getJobSubmissionRequest() {
		NettyRequest request = new NettyRequest(NettyRequestType.JOB_SUBMISSION);
		request.setParamValue("JOB", this);
		return request;
	}
}
