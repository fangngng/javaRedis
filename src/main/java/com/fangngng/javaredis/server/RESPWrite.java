package com.fangngng.javaredis.server;

import static com.fangngng.javaredis.server.RespTypeConst.ARRAY;

public class RESPWrite {


    public String write(RespValue data){
        // *2\r\n$5\r\nhello\r\n$5\r\nworld\r\n
        // $5\r\nhello\r\n
        // 存在这两种命令，*表示后面是array，$表示后面是字符串
        if(data == null){
            return marshalNull();
        }
        switch (data.getType()){
            case "array":
                return marshalArray(data.getArray());
            case "string":
                return marshalString(data.getStr());
            case "error":
                return marshalError(data.getError());
            case "null":
                return marshalNull();
            case "bulk":
                return marshalBulk(data.getBulk());
            default:
                return "";
        }
    }

    private String marshalString(String str){
        StringBuilder builder = new StringBuilder();
        builder.append("+").append(str).append("\\r\\n");
        return builder.toString();
    }

    private String marshalError(String str){
        StringBuilder builder = new StringBuilder();
        builder.append("-").append(str).append("\\r\\n");
        return builder.toString();
    }

    private String marshalNull(){
        return "$-1\\r\\n";
    }

    private String marshalBulk(String bulk){
        StringBuilder builder = new StringBuilder();
        builder.append("$").append(bulk.length()).append("\\r\\n").append(bulk).append("\\r\\n");
        return builder.toString();
    }

    private String marshalArray(RespValue[] array){
        StringBuilder builder = new StringBuilder();
        builder.append(ARRAY).append(array.length).append("\\r\\n");
        for (RespValue value : array) {
            builder.append(write(value));
        }
        return builder.toString();
    }

    public static void main(String[] args) {
        RespValue data = new RespValue();
        data.setType("bulk");
        data.setBulk("hello");
        RespValue[] array = new RespValue[1];
        array[0] = data;
        RespValue a = new RespValue();
        a.setType("array");
        a.setArray(array);
        System.out.println(new RESPWrite().write(a));
    }
}
