package com.fangngng.javaredis.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.CharsetUtil;

public class ServerJ {

    private int port = 3333;

    private AOFPersistent aofPersistent;

    public void setAofPersistent(AOFPersistent aofPersistent) {
        this.aofPersistent = aofPersistent;
    }

    public ServerJ(int port){
        this.port = port;
    }

    public void start() throws Exception{
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline pipeline = ch.pipeline();
                            pipeline.addLast(new StringDecoder());
                            pipeline.addLast(new StringEncoder());
                            pipeline.addLast(new ServerHandler(aofPersistent));
                        }
                    })
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            ChannelFuture f = b.bind(port).sync();
            System.out.println("Server started on port " + port);

            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
            aofPersistent.close();
        }
    }

    public static void main(String[] args) {
        try {
            AOFPersistent aofPersistent = new AOFPersistent();
            aofPersistent.newAOF();
            // read
            aofPersistent.read();


            ServerJ serverJ = new ServerJ(3333);
            serverJ.setAofPersistent(aofPersistent);
            serverJ.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
