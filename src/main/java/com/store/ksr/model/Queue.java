package com.store.ksr.model;

public class Queue {
    private String name;
    private Double bitps;
    private Long time;

    public Queue(String name) {
        this.name = name;
        time = System.currentTimeMillis();
    }


    public String getName() {
        return name;
    }

    public Queue setBitps(Double bitps) {
        this.bitps = bitps;
        return this;
    }

    public Long getTime() {
        return time;
    }

    public Double getBitps() {
        return bitps;
    }
}
