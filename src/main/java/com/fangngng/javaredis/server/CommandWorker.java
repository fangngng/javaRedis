package com.fangngng.javaredis.server;

import java.util.concurrent.*;
import java.util.function.Function;

public class CommandWorker {

    private static final ThreadPoolExecutor worker = new ThreadPoolExecutor(1, 1, 100, TimeUnit.SECONDS, new LinkedBlockingQueue<>(2000), new ThreadPoolExecutor.CallerRunsPolicy());

    public static RespValue workForCommand(RespValue respValue){
        String command = respValue.getArray()[0].getBulk().toLowerCase();
        Function<RespValue, RespValue> handler = CommandHandler.handlers.get(command);

        // 如果是写命令，则放到线程池中
        if(CommandHandler.commandWriteMap.containsKey(command)) {
            Future<RespValue> result = worker.submit(new Callable<RespValue>() {
                @Override
                public RespValue call() throws Exception {
                    try {
                        return handler.apply(respValue);
                    } catch (Exception e) {
                        System.out.println("write command exception， " + respValue);
                    }
                    return null;
                }
            });
            try {
                return result.get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                System.out.println("write command time exception， " + respValue);
            }
        }else{
            // 如果是读命令，则直接执行
            try {
                return handler.apply(respValue);
            } catch (Exception e) {
                System.out.println("read command exception， " + respValue);
            }
        }
        return null;
    }
}
