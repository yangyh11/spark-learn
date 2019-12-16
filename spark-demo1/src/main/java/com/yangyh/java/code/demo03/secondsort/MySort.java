package com.yangyh.java.code.demo03.secondsort;

import java.io.Serializable;

/**
 * @description:
 * @author: yangyh
 * @create: 2019-12-13 10:56
 */
public class MySort implements Comparable<MySort>, Serializable {

    private Integer first;
    private Integer second;

    public MySort(Integer first, Integer second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public int compareTo(MySort that) {
        if (this.first.equals(that.first)) {
            return this.second - that.second;
        } else {
            return this.first - that.first;
        }
    }

    public Integer getFirst() {
        return first;
    }

    public void setFirst(Integer first) {
        this.first = first;
    }

    public Integer getSecond() {
        return second;
    }

    public void setSecond(Integer second) {
        this.second = second;
    }
}
