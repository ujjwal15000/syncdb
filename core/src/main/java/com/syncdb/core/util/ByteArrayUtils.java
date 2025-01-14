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

    public static byte[] convertToByteArray(final long value) {
        byte[] array = new byte[8];
        array[0] = (byte) ((value >> 56) & 0xFF);
        array[1] = (byte) ((value >> 48) & 0xFF);
        array[2] = (byte) ((value >> 40) & 0xFF);
        array[3] = (byte) ((value >> 32) & 0xFF);
        array[4] = (byte) ((value >> 24) & 0xFF);
        array[5] = (byte) ((value >> 16) & 0xFF);
        array[6] = (byte) ((value >> 8) & 0xFF);
        array[7] = (byte) (value & 0xFF);
        return array;
    }

    public static long convertToLong(final byte[] array) {
        if (array.length != 8) {
            throw new IllegalArgumentException("Byte array length must be 8");
        }
        return ((long) (array[0] & 0xFF) << 56)
                | ((long) (array[1] & 0xFF) << 48)
                | ((long) (array[2] & 0xFF) << 40)
                | ((long) (array[3] & 0xFF) << 32)
                | ((long) (array[4] & 0xFF) << 24)
                | ((long) (array[5] & 0xFF) << 16)
                | ((long) (array[6] & 0xFF) << 8)
                | ((long) (array[7] & 0xFF));
    }

    public static void reverse(byte[] array) {
        int start = 0;
        int end = array.length - 1;

        while (start < end) {
            byte temp = array[start];
            array[start] = array[end];
            array[end] = temp;

            start++;
            end--;
        }
    }

}
