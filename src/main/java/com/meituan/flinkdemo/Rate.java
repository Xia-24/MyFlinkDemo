package com.meituan.flinkdemo;

public class Rate{
    public Rate(Long timestamp, String hbdm, Integer num) {
        this.timestamp = timestamp;
        this.hbdm = hbdm;
        this.num = num;
    }

    private Long timestamp;
    private String hbdm;
    private Integer num;

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getHbdm() {
        return hbdm;
    }

    public void setHbdm(String hbdm) {
        this.hbdm = hbdm;
    }

    public Integer getNum() {
        return num;
    }

    public void setNum(Integer num) {
        this.num = num;
    }
}