package com.syncdb.core.util;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ByteArrayUtils {
    public static byte[] convertToByteArray(final int value) {
        byte[] array = new byte[4];
        array[0] = (byte) ((value >> 24) & 0xFF);
        array[1] = (byte) ((value >> 16) & 0xFF);
        array[2] = (byte) ((value >> 8) & 0xFF);
        array[3] = (byte) (value & 0xFF);
        return array;
    }

    public static int convertToInt(final byte[] array) {
        if (array.length != 4) {
            throw new IllegalArgumentException("Byte array length must be 4");
        }
        return ((array[0] & 0xFF) << 24)
                | ((array[1] & 0xFF) << 16)
                | ((array[2] & 0xFF) << 8)
                | (array[3] & 0xFF);
    }

}
