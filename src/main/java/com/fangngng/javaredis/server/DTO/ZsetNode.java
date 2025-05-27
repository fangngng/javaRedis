package com.fangngng.javaredis.server.DTO;

public class ZsetNode {

    private String value;

    private String score;

    public ZsetNode(String value, String score) {
        this.value = value;
        this.score = score;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getScore() {
        return score;
    }

    public void setScore(String score) {
        this.score = score;
    }
}
