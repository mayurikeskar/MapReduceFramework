package com.mr.hw.cluster.slave;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.log4j.Logger;

import com.mr.hw.cluster.Utils.Constants;
import com.mr.hw.cluster.request.NettyRequest;
import com.mr.hw.cluster.request.NettyRequestType;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

/**
 * Slave node class
 * @author kamlendrak
 *
 */
public class Slave {

	private static final int MASTER_PORT = Constants.PORT;
	private static final Logger LOGGER = Logger.getLogger(Slave.class);

	/**
	 * @param args
	 * @throws InterruptedException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws InterruptedException, IOException {
		BufferedReader br = new BufferedReader(new FileReader(new File("masterInstance.txt")));
		String line = null, masterIp =null;
		while((line = br.readLine())!=null){
			masterIp = line;
		}
		LOGGER.info("Starting slave");
		EventLoopGroup worker = new NioEventLoopGroup();
		Bootstrap b = new Bootstrap().group(worker).channel(NioSocketChannel.class)
				.option(ChannelOption.SO_KEEPALIVE, true)
				.handler(new SlaveInitializer(args[0]));
		Channel ch = b.connect(masterIp, MASTER_PORT).sync().channel();
		ch.writeAndFlush(new NettyRequest(NettyRequestType.SLAVE_REGISTRATION));
	}
}
