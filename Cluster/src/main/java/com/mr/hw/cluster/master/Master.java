/**
 * 
 */
package com.mr.hw.cluster.master;

import org.apache.log4j.Logger;

import com.mr.hw.cluster.Utils.Constants;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

/**
 * Class to start master node.
 * @author kamlendrak
 *
 */
public class Master {

	private static final int PORT = Constants.PORT;
	private static final Logger LOGGER = Logger.getLogger(Master.class);

	/**
	 * Start master
	 * @param args
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws InterruptedException {
		EventLoopGroup master = new NioEventLoopGroup(1);
		EventLoopGroup worker = new NioEventLoopGroup();
		try {
			LOGGER.info("bootstraping master");
			ServerBootstrap b = new ServerBootstrap().group(master, worker)
					.channel(NioServerSocketChannel.class)
					.childHandler(new MasterInitializer(args[0]))
					.childOption(ChannelOption.SO_KEEPALIVE, true)
					.option(ChannelOption.SO_KEEPALIVE, true);
			LOGGER.info("Binding master to port");
			b.bind(PORT).channel().closeFuture().sync();
		} finally {
			master.shutdownGracefully();
			worker.shutdownGracefully();
		}
	}
}
