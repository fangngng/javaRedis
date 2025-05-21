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

        RespValue resp = handler.apply(read);

        // write to aof
        aofForCommand(command, read);

        ctx.writeAndFlush(new RESPWrite().write(resp));
    }

    private void aofForCommand(String command, RespValue respValue){
        if("set".equalsIgnoreCase(command) || "hset".equalsIgnoreCase(command) || "del".equalsIgnoreCase(command)){
            this.aofPersistent.write(new RESPWrite().write(respValue).getBytes(StandardCharsets.UTF_8));
        }
        if("expire".equalsIgnoreCase(command)){
            String key = respValue.getArray()[1].getBulk();
            Long expire = CommandHandler.expireMap.get(key);
            RespValue aofValue = new RespValue();
            aofValue.setType("array");
            aofValue.setArray(new RespValue[3]);
            aofValue.getArray()[0] = RespValue.bulk("pexpireat");
            aofValue.getArray()[1] = respValue.getArray()[1];
            aofValue.getArray()[2] = RespValue.bulk(String.valueOf(expire));
            this.aofPersistent.write(new RESPWrite().write(aofValue).getBytes(StandardCharsets.UTF_8));
        }
        if("pexpireat".equalsIgnoreCase(command)){
            this.aofPersistent.write(new RESPWrite().write(respValue).getBytes(StandardCharsets.UTF_8));
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        cause.printStackTrace();
        ctx.close();
    }
}
