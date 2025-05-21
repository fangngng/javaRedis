package com.fangngng.javaredis.server;


import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;

import java.util.Scanner;

public class ClientJ {

    public static void main(String[] args) {
        String host = "127.0.0.1";
        Integer port = 3333;

        EventLoopGroup group = new NioEventLoopGroup();

        try {
            Bootstrap b = new Bootstrap();
            b.group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline pipeline = ch.pipeline();
                            pipeline.addLast(new StringDecoder());
                            pipeline.addLast(new StringEncoder());
                            pipeline.addLast(new ClientHandler());
                        }
                    });

            Channel channel = b.connect(host, port).sync().channel();
            System.out.println("Connected to server. Type 'exit' to quit.");

            Scanner scanner = new Scanner(System.in);
            while (scanner.hasNextLine()){
                String str = scanner.nextLine();
                channel.writeAndFlush(new RESPWrite().write(convert(str)));
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            group.shutdownGracefully();
        }
    }

    public static RespValue convert(String data){
        String[] split = data.split(" ");
        RespValue respValue = new RespValue();
        respValue.setType("array");

        RespValue[] array = new RespValue[split.length];
        int j = 0;
        for (String s : split) {
            RespValue value = new RespValue("bulk", null, s);
            array[j++] = value;
        }
        respValue.setArray(array);
        return respValue;
    }
}
