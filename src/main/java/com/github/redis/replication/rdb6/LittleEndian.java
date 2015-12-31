package com.github.redis.replication.rdb6;

/**
 * Created by wens on 15-12-24.
 */
public class LittleEndian {

    public static long uint8(byte[] b) {
        return b[0] & 0xff;
    }

    public static long uint16(byte[] b) {
        return b[0] & 0xff | (b[1] & 0xff) << 8;
    }

    public static long uint32(byte[] b) {
        return (long) (b[0] & 0xff) | ((long) (b[1] & 0xff)) << 8 | ((long) (b[2] & 0xff)) << 16 | ((long) (b[3] & 0xff)) << 24;
    }

    //maybe overflow!!!
    public static long uint64(byte[] b) {
        return (long) (b[0] & 0xff) | ((long) (b[1] & 0xff)) << 8 | ((long) (b[2] & 0xff)) << 16 | ((long) (b[3] & 0xff)) << 24 |
                ((long) (b[4] & 0xff)) << 32 | ((long) (b[5] & 0xff)) << 40 | ((long) (b[6] & 0xff)) << 48 | ((long) (b[7] & 0xff)) << 56;
    }


    public static int int8(byte[] b) {
        return (int) b[0];
    }

    public static int int16(byte[] b) {
        return (int) b[0] | ((int) b[1]) << 8;
    }

    public static int int32(byte[] b) {
        return ((int) b[0]) | ((int) b[1]) << 8 | ((int) b[2]) << 16 | ((int) b[3]) << 24;
    }

    public static long int64(byte[] b) {
        return (long) (b[0]) | ((long) (b[1])) << 8 | ((long) (b[2])) << 16 | ((long) (b[3])) << 24 |
                ((long) (b[4])) << 32 | ((long) (b[5])) << 40 | ((long) (b[6])) << 48 | ((long) (b[7])) << 56;
    }
}
