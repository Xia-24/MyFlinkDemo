package com.meituan.flinkdemo.Entity;

import java.io.Serializable;

public class Myclass implements Serializable {
    public Myclass() {
        this.age =1;
        this.name = "name";
        this.sex = false;
    }

    public Myclass(Integer age, Boolean sex, String name) {
        this.age = age;
        this.sex = sex;
        this.name = name;
    }

    private Integer age;
    private Boolean sex;
    private String name;

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public Boolean getSex() {
        return sex;
    }

    public void setSex(Boolean sex) {
        this.sex = sex;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
