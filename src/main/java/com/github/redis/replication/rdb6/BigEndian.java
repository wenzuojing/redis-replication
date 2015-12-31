package com.github.redis.replication.rdb6;

/**
 * Created by wens on 15-12-24.
 */
public class BigEndian {

    public static int uint32(byte[] b) {
        return b[3] & 0xff | (b[2] & 0xff) << 8 | (b[1] & 0xff) << 16 | (b[0] & 0xff) << 24;
    }

}
