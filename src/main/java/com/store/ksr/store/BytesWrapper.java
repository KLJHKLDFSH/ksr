package com.store.ksr.store;

import org.apache.kafka.common.utils.Bytes;

import java.io.Serializable;

public class BytesWrapper implements Comparable<BytesWrapper>,Serializable {

    private final byte[] ser;

    private BytesWrapper(Bytes bytes) {
        ser = bytes.get();
    }

    public static BytesWrapper wrap(Bytes bytes){
        return new BytesWrapper(bytes);
    }

    public Bytes get() {
        return Bytes.wrap(ser);
    }

    @Override
    public int compareTo(BytesWrapper o) {
        return get().compareTo(o.get());
    }
}
