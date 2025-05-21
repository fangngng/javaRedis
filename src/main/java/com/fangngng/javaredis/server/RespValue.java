package com.fangngng.javaredis.server;

import java.util.Arrays;

public class RespValue {

    RespValue(){}

    RespValue(String type, String str){
        this.type = type;
        this.str = str;
    }

    RespValue(String type, String str, String bulk){
        this.type = type;
        this.str = str;
        this.bulk = bulk;
    }

    private String type;

    private RespValue[] array;

    private String str;

    private Integer num;

    private String bulk;

    private String error;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public RespValue[] getArray() {
        return array;
    }

    public void setArray(RespValue[] array) {
        this.array = array;
    }

    public String getStr() {
        return str;
    }

    public void setStr(String str) {
        this.str = str;
    }

    public Integer getNum() {
        return num;
    }

    public void setNum(Integer num) {
        this.num = num;
    }

    public String getBulk() {
        return bulk;
    }

    public void setBulk(String bulk) {
        this.bulk = bulk;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("type:" + this.type);
        if(this.array != null){
            builder.append(",array:" + Arrays.toString(this.array));
        }
        if(this.bulk != null){
            builder.append(",bulk:" + this.bulk);
        }
        if(this.str != null){
            builder.append(",str:" + this.str);
        }
        if(this.num != null){
            builder.append(",num:" + this.num);
        }
        if(this.error != null){
            builder.append(",error:" + this.error);
        }
        return builder.toString();
    }

    public static RespValue error(String error){
        RespValue value = new RespValue();
        value.setType("error");
        value.setError(error);
        return value;
    }

    public static RespValue ok(){
        RespValue value = new RespValue();
        value.setType("string");
        value.setStr("ok");
        return value;
    }

    public static RespValue nul(){
        RespValue value = new RespValue();
        value.setType("string");
        value.setStr(null);
        return value;
    }

    public static RespValue bulk(String bulk){
        RespValue value = new RespValue();
        value.setType("bulk");
        value.setBulk(bulk);
        return value;
    }
}
