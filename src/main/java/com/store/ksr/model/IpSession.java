package com.store.ksr.model;

public class IpSession {
    private final String name;
    private Double bitps;

    private Long start;
    private Long end;
    private String startStr;
    private String endStr;

    public IpSession(String name, Double bitps) {
        this.name = name;
        this.bitps = bitps;
    }

    public String getName() {
        return name;
    }

    public Double getBitps() {
        return bitps;
    }

    public void setTime(Long start, Long end){
        this.start = start;
        this.end = end;

    }

    public IpSession setBitps(Double bitps) {
        this.bitps = bitps;
        return this;
    }

    public Long getStart() {
        return start;
    }

    public Long getEnd() {
        return end;
    }
}
