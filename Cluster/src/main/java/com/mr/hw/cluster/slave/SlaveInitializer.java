/**
 * 
 */
package com.mr.hw.cluster.slave;

import com.mr.hw.cluster.request.NettyRequest;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;

/**
 * Initialise channel
 * @author kamlendrak
 *
 */
public class SlaveInitializer extends ChannelInitializer<SocketChannel> {
	
	private static String MODE;
	
	public SlaveInitializer(String mode) {
		if(mode == null || mode.length() == 0) {
			MODE = mode;
		}
	}

	@Override
	protected void initChannel(SocketChannel channel) throws Exception {
		ChannelPipeline pipeline = channel.pipeline();
		pipeline.addLast(new ObjectDecoder(ClassResolvers.weakCachingResolver(NettyRequest.class.getClassLoader())));
		pipeline.addLast(new ObjectEncoder());
		pipeline.addLast(new SlaveChannelhandler(channel, MODE));
	}
}
