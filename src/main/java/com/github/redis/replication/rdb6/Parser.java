package com.github.redis.replication.rdb6;

import java.io.InputStream;

/**
 * Created by wens on 15-12-21.
 */
public class Parser extends Reader {

    public final static int Version = 6;

    private int db;

    public Parser(InputStream inputStream) {
        super(inputStream);
    }

    public void header() {

        byte[] header = new byte[9];
        readFull(header);
        if (!"REDIS".equals(new String(header, 0, 5))) {
            throw new RuntimeException("verify magic string, invalid file format");
        }
        int version = Integer.parseInt(new String(header, 5, 4));
        if (version <= 0 || version > Version) {
            throw new RuntimeException(String.format("verify version, invalid RDB version number %d", version));
        }
    }

    public void footer() {
        //System.out.println(readUint64());
        readBytes(8);//8bit crc sum
    }

    public static class BinEntry {
        int db;
        String key;
        Object value;
        long expireAt;

        public BinEntry() {
        }

        public BinEntry(int db, String key, Object value, long expireAt) {
            this.db = db;
            this.key = key;
            this.value = value;
            this.expireAt = expireAt;
        }

        @Override
        public String toString() {
            return "BinEntry{" +
                    "db=" + db +
                    ", key='" + key + '\'' +
                    ", value=" + value +
                    ", expireAt=" + expireAt +
                    '}';
        }
    }


    public BinEntry nextBinEntry() {
        BinEntry entry = new BinEntry();
        for (; ; ) {
            int t = readByte() & 0xff ;
            switch (t) {
                case rdbFlagExpiryMS:
                    long ttlms = readUint64();
                    entry.expireAt = ttlms;
                    break;
                case rdbFlagExpiry:
                    long ttls = readUint32();
                    entry.expireAt = ttls * 1000;
                    break;
                case rdbFlagSelectDB:
                    int dbnum = readLength();
                    this.db = dbnum;
                    break;
                case rdbFlagEOF:
                    return null;
                default:
                    String key = readString();
                    Object val = readObjectValue(t);
                    entry.db = this.db;
                    entry.key = key;
                    entry.value = val;
                    return entry;
            }
        }
    }
}
