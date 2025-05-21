package com.fangngng.javaredis.server;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.internal.StringUtil;

import java.nio.charset.StandardCharsets;
import java.util.function.Function;

public class ServerHandler extends SimpleChannelInboundHandler<String> {

    private AOFPersistent aofPersistent;

    public ServerHandler(){}

    public ServerHandler(AOFPersistent aofPersistent){
        this.aofPersistent = aofPersistent;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
        System.out.println("Received from client: " + msg);
        RespValue read = new RESPRead().read(msg);
        System.out.println("deserialize: " + read);

        if(read == null){
            System.out.println("error read");
            ctx.writeAndFlush(new RESPWrite().write(RespValue.error("error read")));
            return;
        }
        if(!"array".equals(read.getType()) || read.getArray().length < 1){
            System.out.println("error type");
            ctx.writeAndFlush(new RESPWrite().write(RespValue.error("error type")));
            return;
        }


        String command = read.getArray()[0].getBulk().toLowerCase();
        Function<RespValue, RespValue> handler = CommandHandler.handlers.get(command);
        if(handler == null){
            System.out.println("error command");
            ctx.writeAndFlush(new RESPWrite().write(RespValue.error("error command")));
            return;
        }

        // write to aof
        if ("set".equalsIgnoreCase(command) || "hset".equalsIgnoreCase(command)) {
            this.aofPersistent.write(msg.getBytes(StandardCharsets.UTF_8));
        }

        RespValue resp = handler.apply(read);
        ctx.writeAndFlush(new RESPWrite().write(resp));
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }
}
