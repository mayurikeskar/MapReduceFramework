package com.mr.hw.cluster.master;

import com.mr.hw.cluster.request.NettyRequest;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;

/**
 * initializes channel
 * @author kamlendrak
 *
 */
public class MasterInitializer extends ChannelInitializer<SocketChannel> {

	private String mode;
	
	public MasterInitializer(String mode) {
		this.mode = mode;
	}
	
	@Override
	protected void initChannel(SocketChannel channel) throws Exception {
		ChannelPipeline pipeline = channel.pipeline();
		pipeline.addLast(new ObjectDecoder(ClassResolvers.weakCachingResolver(NettyRequest.class.getClassLoader())));
		pipeline.addLast(new ObjectEncoder());
		pipeline.addLast(new MasterChannelhandler(mode));
	}
}
