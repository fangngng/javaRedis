package com.fangngng.javaredis.server;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

public class CommandHandler {

    public static Map<String, Function<RespValue, RespValue>> handlers = new HashMap<>();

    public static Map<String, String> map = new HashMap<>();
    public static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public static Map<String, HashMap<String, String>> hmap = new HashMap<>();
    public static ReentrantReadWriteLock hmapLock = new ReentrantReadWriteLock();

    public static Map<String, Long> expireMap = new HashMap<>();

    static{
        // ping command
        Function<RespValue, RespValue> ping = new Function<RespValue, RespValue>() {
            @Override
            public RespValue apply(RespValue respValue) {
                if(respValue != null && respValue.getArray().length > 1){
                    return new RespValue("string", respValue.getArray()[1].getBulk());
                }
                return new RespValue("string", "pong");
            }
        };

        // set command
        // todo expire
        // todo lock
        Function<RespValue, RespValue> set = new Function<RespValue, RespValue>() {
            @Override
            public RespValue apply(RespValue respValue) {
                if(respValue == null || respValue.getArray().length < 3){
                    return RespValue.error("error command format");
                }

                String key = respValue.getArray()[1].getBulk();
                String value = respValue.getArray()[2].getBulk();

                expireIfNeeded(key);

                ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();
                writeLock.lock();
                try {
                    map.put(key, value);
                }finally {
                    writeLock.unlock();
                }

                return RespValue.ok();
            }
        };

        // get command
        Function<RespValue, RespValue> get = new Function<RespValue, RespValue>() {
            @Override
            public RespValue apply(RespValue respValue) {
                if(respValue == null || respValue.getArray().length != 2){
                    return RespValue.error("error command format");
                }

                String key = respValue.getArray()[1].getBulk();
                String value = null;

                expireIfNeeded(key);

                ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
                readLock.lock();
                try {
                    value = map.get(key);
                }finally {
                    readLock.unlock();
                }
                return new RespValue("string", value);
            }
        };

        // hset
        Function<RespValue, RespValue> hset = new Function<RespValue, RespValue>() {
            @Override
            public RespValue apply(RespValue respValue) {
                if(respValue == null || respValue.getArray().length < 4){
                    return RespValue.error("error command format");
                }

                String hash = respValue.getArray()[1].getBulk();
                String key = respValue.getArray()[2].getBulk();
                String value = respValue.getArray()[3].getBulk();

                expireIfNeeded(hash);

                ReentrantReadWriteLock.WriteLock writeLock = hmapLock.writeLock();
                writeLock.lock();
                try {
                    hmap.putIfAbsent(hash, new HashMap<>());
                    hmap.get(hash).put(key, value);
                } finally {
                    writeLock.unlock();
                }

                return RespValue.ok();
            }
        };

        Function<RespValue, RespValue> hget = new Function<RespValue, RespValue>() {
            @Override
            public RespValue apply(RespValue respValue) {
                if(respValue == null || respValue.getArray().length < 3){
                    return RespValue.error("error command format");
                }

                String hash = respValue.getArray()[1].getBulk();
                String key = respValue.getArray()[2].getBulk();

                expireIfNeeded(hash);

                ReentrantReadWriteLock.ReadLock readLock = hmapLock.readLock();
                readLock.lock();
                HashMap<String, String> hashMap = null;
                try {
                    hashMap = hmap.get(hash);
                } finally {
                    readLock.unlock();
                }

                if(hashMap != null){
                    return new RespValue("string", hashMap.get(key));
                }
                return RespValue.nul();
            }
        };

        Function<RespValue, RespValue> hgetall = new Function<RespValue, RespValue>() {
            @Override
            public RespValue apply(RespValue respValue) {
                if(respValue == null || respValue.getArray().length < 2){
                    return RespValue.error("error command format");
                }

                String hash = respValue.getArray()[1].getBulk();

                expireIfNeeded(hash);

                ReentrantReadWriteLock.ReadLock readLock = hmapLock.readLock();
                readLock.lock();
                HashMap<String, String> hashMap = null;
                try {
                    hashMap = hmap.get(hash);
                } finally {
                    readLock.unlock();
                }

                if(hashMap != null){
                    RespValue data = new RespValue();
                    data.setType("array");
                    
                    RespValue[] array = new RespValue[hashMap.size()*2];
                    int j=0;
                    for (Map.Entry<String, String> entry : hashMap.entrySet()) {
                        array[j++] = RespValue.bulk(entry.getKey());
                        array[j++] = RespValue.bulk(entry.getValue());
                    }

                    data.setArray(array);
                    return data;
                }
                return RespValue.nul();
            }
        };

        // PEXPIREAT key timestamp
        Function<RespValue, RespValue> pexpireat = new Function<RespValue, RespValue>() {
            @Override
            public RespValue apply(RespValue respValue) {
                if(respValue == null || respValue.getArray().length != 3){
                    return RespValue.error("error command format");
                }

                String key = respValue.getArray()[1].getBulk();
                String expire = respValue.getArray()[2].getBulk();

                expireIfNeeded(key);

                expireMap.put(key, Long.parseLong(expire));

                return RespValue.ok();
            }
        };


        // expire key seconds
        Function<RespValue, RespValue> expire = new Function<RespValue, RespValue>() {
            @Override
            public RespValue apply(RespValue respValue) {
                if(respValue == null || respValue.getArray().length != 3){
                    return RespValue.error("error command format");
                }

                String key = respValue.getArray()[1].getBulk();
                String expire = respValue.getArray()[2].getBulk();

                expireIfNeeded(key);

                long currentTimeMillis = System.currentTimeMillis();
                expireMap.put(key, currentTimeMillis + Integer.parseInt(expire)* 1000L);

                return RespValue.ok();
            }
        };

        // delete
        Function<RespValue, RespValue> delete = new Function<RespValue, RespValue>() {
            @Override
            public RespValue apply(RespValue respValue) {
                if(respValue == null || respValue.getArray().length != 2){
                    return RespValue.error("error command format");
                }

                String key = respValue.getArray()[1].getBulk();
                map.remove(key);
                hmap.remove(key);
                expireMap.remove(key);

                return RespValue.ok();
            }
        };

        // ttl key
        Function<RespValue, RespValue> ttl = new Function<RespValue, RespValue>() {
            @Override
            public RespValue apply(RespValue respValue) {
                if(respValue == null || respValue.getArray().length != 2){
                    return RespValue.error("error command format");
                }

                String key = respValue.getArray()[1].getBulk();
                long currentTimeMillis = System.currentTimeMillis();
                if(expireMap.containsKey(key)){
                    long seconds = (expireMap.get(key) - currentTimeMillis) / 1000L;
                    if(seconds < 0){
                        delete.apply(respValue);
                        return RespValue.error("key expired");
                    }else{
                        return RespValue.bulk(String.valueOf(seconds));
                    }
                }

                return RespValue.bulk("-1");
            }
        };


        handlers.put("ping", ping);
        handlers.put("set", set);
        handlers.put("get", get);
        handlers.put("hset", hset);
        handlers.put("hget", hget);
        handlers.put("hgetall", hgetall);
        handlers.put("pexpireat", pexpireat);
        handlers.put("expire", expire);
        handlers.put("ttl", ttl);
        handlers.put("del", delete);
    }

    public static void expireIfNeeded(String key){
        if(expireMap.containsKey(key) && expireMap.get(key) < System.currentTimeMillis()){
            map.remove(key);
            hmap.remove(key);
            expireMap.remove(key);
        }
    }


}
