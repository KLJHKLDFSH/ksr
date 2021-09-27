package com.store.ksr.model;

public class Line {

    private String name;
    private double max;
    private double min;
    private double rx;
    private double tx;
    private long time;

    public Line(String name, double max, double min, double rx, double tx, long time) {
        this.name = name;
        this.max = max;
        this.min = min;
        this.rx = rx;
        this.tx = tx;
        this.time = time;
    }

    public String getName() {
        return name;
    }

    public double getMax() {
        return max;
    }

    public double getMin() {
        return min;
    }

    public double getRx() {
        return rx;
    }

    public double getTx() {
        return tx;
    }

    public long getTime() {
        return time;
    }
}
