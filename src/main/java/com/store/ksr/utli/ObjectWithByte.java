package com.store.ksr.utli;

import java.io.*;

public class ObjectWithByte {

    public static byte[] toByteArray(Object data){
        byte[] bytes = null;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(data);
            oos.flush();
            bytes = baos.toByteArray();
            oos.close();
            baos.close();
        } catch (IOException e) {

            e.printStackTrace();
        } finally {
            try {
                baos.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return bytes;
    }


    public static Object toObject(byte[] data){
        Object t = null;
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream (data);
            ObjectInputStream ois = new ObjectInputStream (bis);
            t = ois.readObject();
            ois.close();
            bis.close();
        } catch (IOException | ClassNotFoundException ex) {
            ex.printStackTrace();
        }
        return t;
    }
}
