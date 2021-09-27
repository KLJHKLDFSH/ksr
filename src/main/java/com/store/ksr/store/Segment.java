package com.store.ksr.store;

import org.apache.kafka.common.utils.Bytes;

import java.io.Serializable;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class Segment implements Serializable {

    private final ConcurrentNavigableMap<Bytes, byte[]> map = new ConcurrentSkipListMap<>();

    public Segment(Bytes key, byte[] value) {
        map.put(key,value);
    }

}

