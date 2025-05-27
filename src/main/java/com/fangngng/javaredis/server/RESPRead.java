package com.fangngng.javaredis.server;

import java.nio.charset.StandardCharsets;

public class RESPRead {

    public RespValue read(String data){
        // *2\r\n$5\r\nhello\r\n$5\r\nworld\r\n
        // $5\r\nhello\r\n
        // 存在这两种命令，*表示后面是array，$表示后面是字符串

        try {
            String[] split = data.split("\\\\r\\\\n");
            byte[] bytes = split[0].getBytes(StandardCharsets.UTF_8);
            if(bytes[0] =='*'){
                return readArray(split);
            }else if (bytes[0] =='$'){
                return readBulk(split);
            }else if (bytes[0] =='+' || bytes[0] =='-'){
                return readStr(split);
            }else{
                System.out.println("error command type");
                return new RespValue();
            }
        } catch (Exception e) {
            System.out.println("read command error");
        }
        return RespValue.nul();
    }

    private RespValue readArray(String[] split){
        int size = Character.getNumericValue(split[0].toCharArray()[1]);
        RespValue data = new RespValue();
        data.setType("array");
        int j=0;
        RespValue[] array = new RespValue[size];
        for (int i = 2; i < split.length; i=i+2) {
            RespValue value = new RespValue();
            value.setType("bulk");
            value.setBulk(split[i]);
            array[j++]=value;
        }
        data.setArray(array);
        return data;
    }

    private RespValue readBulk(String[] split){
        RespValue data = new RespValue();
        data.setType("bulk");
        data.setBulk(split[1]);
        return data;
    }

    private RespValue readStr(String[] split){
        RespValue data = new RespValue();
        data.setType("string");
        data.setStr(split[0].replace("-", "").replace("+", ""));
        return data;
    }


    public static void main(String[] args) {
        RESPRead respRead = new RESPRead();
        RespValue read = respRead.read("*2\\r\\n$5\\r\\nhello\\r\\n$5\\r\\nworld\\r\\n");
        System.out.println(read);
        RespValue read2 = respRead.read("$5\\r\\nhello\\r\\n");
        System.out.println(read2);
    }
}
