package com.github.redis.replication.rdb6;


import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by wens on 15-12-21.
 */
public class Reader {

    public final int rdbTypeString = 0;
    public final int rdbTypeList = 1;
    public final int rdbTypeSet = 2;
    public final int rdbTypeZSet = 3;
    public final int rdbTypeHash = 4;

    public final int rdbTypeHashZipmap = 9;
    public final int rdbTypeListZiplist = 10;
    public final int rdbTypeSetIntset = 11;
    public final int rdbTypeZSetZiplist = 12;
    public final int rdbTypeHashZiplist = 13;

    public final int rdbFlagExpiryMS = 0xfc;
    public final int rdbFlagExpiry = 0xfd;
    public final int rdbFlagSelectDB = 0xfe;
    public final int rdbFlagEOF = 0xff;


    public final int rdb6bitLen = 0;
    public final int rdb14bitLen = 1;
    public final int rdb32bitLen = 2;
    public final int rdbEncVal = 3;

    public final int rdbEncInt8 = 0;
    public final int rdbEncInt16 = 1;
    public final int rdbEncInt32 = 2;
    public final int rdbEncLZF = 3;

    public final int rdbZiplist6bitlenString = 0;
    public final int rdbZiplist14bitlenString = 1;
    public final int rdbZiplist32bitlenString = 2;

    public final int rdbZiplistInt16 = 0xc0;
    public final int rdbZiplistInt32 = 0xd0;
    public final int rdbZiplistInt64 = 0xe0;
    public final int rdbZiplistInt24 = 0xf0;
    public final int rdbZiplistInt8 = 0xfe;
    public final int rdbZiplistInt4 = 15;


    private InputStream inputStream;


    public Reader(InputStream inputStream) {
        this.inputStream = inputStream ;
    }


    public void readFull(byte[] buf) {
        int c = 0;
        while (c != buf.length) {
            int n = 0;
            try {
                n = inputStream.read(buf, c, buf.length - c);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            if (n == -1) {
                throw new RuntimeException("Read " + buf.length + " bytes , but only " + c + " already read.");
            }
            c += n;
        }
    }


    public Object readObjectValue(int type) {
        Object obj = null;
        switch (type) {
            default:
                throw new RuntimeException(String.format("unknown object-type %02x", type));
            case rdbTypeHashZipmap:
                obj = readHashZipmap(readStringBytes());
                break;
            case rdbTypeListZiplist:
                obj = readListZiplist(readStringBytes());
                break;
            case rdbTypeSetIntset:
                obj = readSetIntset(readStringBytes());
                break;
            case rdbTypeZSetZiplist:
                obj = readZSetZiplist(readStringBytes());
                break;
            case rdbTypeHashZiplist:
                obj = readHashZiplist(readStringBytes());
                break;
            case rdbTypeString:
                obj = readString();
                break;
            case rdbTypeList:
                int nl = readLength();
                List<String> list = new ArrayList<>(nl);
                for (int i = 0; i < nl; i++) {
                    list.add(readString());
                }
                obj = list;
                break;
            case rdbTypeSet:
                int ns = readLength();
                Set<String> set = new HashSet<>(ns);
                for (int i = 0; i < ns; i++) {
                    set.add(readString());
                }
                obj = set;
                break;
            case rdbTypeZSet:
                int nzs = readLength();
                Map<String, Double> zset = new HashMap<>();
                for (int i = 0; i < nzs; i++) {
                    zset.put(readString(), readFloat64());
                }
                obj = zset;
                break;
            case rdbTypeHash:
                Map<String, String> hash = new HashMap<>();
                int nh = readLength();
                for (int i = 0; i < nh; i++) {
                    hash.put(readString(), readString());
                }
                obj = hash;
                break;
        }
        return obj;
    }

    private Set<String> readSetIntset(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        Set<String> set = new HashSet<>();

        byte[] intSizeBytes = new byte[4];
        buf.get(intSizeBytes);
        int intSize = (int) LittleEndian.uint32(intSizeBytes);

        if (intSize != 2 && intSize != 4 && intSize != 8) {
            throw new RuntimeException(String.format("rdb: unknown intset encoding: %d", intSize));
        }

        byte[] lenBytes = new byte[4];
        buf.get(lenBytes);
        int cardinality = (int) LittleEndian.uint32(lenBytes);
        for (int i = 0; i < cardinality; i++) {
            byte[] intBytes = new byte[intSize];

            buf.get(intBytes);
            String intString = null;
            switch (intSize) {
                case 2:
                    intString = String.valueOf(LittleEndian.uint16(intBytes));
                    break;
                case 4:
                    intString = String.valueOf(LittleEndian.uint32(intBytes));
                    break;
                case 8:
                    intString = String.valueOf(LittleEndian.uint64(intBytes));
                    break;
            }
            set.add(intString);
        }
        return set;
    }

    private Map<String, Double> readZSetZiplist(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        Map<String, Double> zset = new HashMap<>();
        int cardinality = readZiplistLength(buf);
        cardinality /= 2;
        for (int i = 0; i < cardinality; i++) {
            String member = readZiplistEntry(buf);
            String scoreStr = readZiplistEntry(buf);
            zset.put(member, Double.parseDouble(scoreStr));
        }
        return zset;
    }

    private Map<String, String> readHashZiplist(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        Map<String, String> map = new HashMap<>();
        int length = readZiplistLength(buf);
        length /= 2;

        for (int i = 0; i < length; i++) {
            String field = readZiplistEntry(buf);
            String value = readZiplistEntry(buf);
            map.put(field, value);
        }
        return map;
    }

    private Map<String, String> readHashZipmap(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        Map<String, String> map = new HashMap<>();
        int length;

        int lenByte = buf.get();

        if (lenByte >= 254) { // we need to count the items manually
            length = countZipmapItems(buf);
            length /= 2;
        } else {
            length = lenByte;
        }
        for (int i = 0; i < length; i++) {
            String field = readZipmapItem(buf, false);
            String value = readZipmapItem(buf, true);
            map.put(field, value);
        }
        return map;
    }


    public String readZipmapItem(ByteBuffer buf, Boolean readFree) {
        Tuple<Integer, Integer> tuple = readZipmapItemLength(buf, readFree);
        if (tuple.getA() == -1) {
            return null;
        }
        byte[] b = new byte[tuple.getA()];
        buf.get(b);
        buf.position(buf.position() + tuple.getB());
        return new String(b);
    }

    public int countZipmapItems(ByteBuffer buf) {
        buf.mark();
        int n = 0;
        for (; ; ) {
            Tuple<Integer, Integer> tuple = readZipmapItemLength(buf, n % 2 != 0);
            if (tuple.getA() == -1) {
                break;
            }
            buf.position(buf.position() + tuple.getA() + tuple.getB());
            n++;
        }
        buf.reset();
        return n;
    }

    public Tuple<Integer/*len*/, Integer/*free*/> readZipmapItemLength(ByteBuffer buf, Boolean readFree) {
        int b = buf.get();

        switch (b) {
            case 253:
                byte[] s = new byte[5];
                buf.get(s);
                return Tuple.of(BigEndian.uint32(s), s[4]);
            case 254:
                return Tuple.of(0, 0);
            case 255:
                return Tuple.of(-1, 0);
        }
        int free = 0;
        if (readFree) {
            free = buf.get();
        }
        return Tuple.of(b, free);
    }

    private List<String> readListZiplist(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);

        int length = readZiplistLength(buf);
        List<String> list = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            list.add(readZiplistEntry(buf));
        }
        return list;
    }


    private int readZiplistLength(ByteBuffer buf) {
        buf.position(8);// skip the zlbytes and zltail
        return (int) LittleEndian.uint16(new byte[]{buf.get(), buf.get()});
    }


    private String readZiplistEntry(ByteBuffer buf) {
        int prevLen = buf.get() & 0xff ;

        if (prevLen == 254) {
            buf.position(buf.position() + 4);// skip the 4-byte prevlen
        }

        int header = buf.get() & 0xff;

        int strLen = 0;

        switch ((header & 0x00C0) >> 6) {
            case rdbZiplist6bitlenString:
                strLen = header & 0x3f;
                break;
            case rdbZiplist14bitlenString:
                strLen = (header & 0x3f) << 8 | (int) buf.get();
                break;
            case rdbZiplist32bitlenString:
                byte[] b = new byte[4];
                buf.get(b);
                strLen = BigEndian.uint32(b);
                break;
        }

        if (strLen != 0) {
            byte[] b = new byte[strLen];
            buf.get(b);
            return new String(b);
        }

        switch (header) {
            case rdbZiplistInt16: {
                byte[] b = new byte[2];
                buf.get(b);
                return String.valueOf(LittleEndian.uint16(b));
            }
            case rdbZiplistInt32: {
                byte[] b = new byte[4];
                buf.get(b);
                return String.valueOf(LittleEndian.uint32(b));
            }
            case rdbZiplistInt64: {
                byte[] b = new byte[8];
                buf.get(b);
                return String.valueOf(LittleEndian.uint64(b));
            }
            case rdbZiplistInt24: {
                byte[] b = new byte[4];
                buf.get(b);
                return String.valueOf(LittleEndian.uint32(b) >> 8);
            }
            case rdbZiplistInt8: {
                return String.valueOf(LittleEndian.uint8(new byte[]{buf.get()}));
            }
            default:
                if ((header & 0x00f0) >> 4 == rdbZiplistInt4) {
                    return String.valueOf((header & 0x0f) - 1);
                } else {
                    throw new RuntimeException("unknown ziplist header : " + header);
                }
        }
    }


    public String readString() {
        Tuple<Integer, Boolean> tuple = readEncodedLength();
        int length = tuple.getA();
        boolean encoded = tuple.getB();

        if (!encoded) {
            return new String(readBytes(length));
        }
        int t = length;
        switch (t) {
            default:
                throw new RuntimeException(String.format("invalid encoded-string %02x", t));
            case rdbEncInt8:
                return String.valueOf((long) readInt8());
            case rdbEncInt16:
                return String.valueOf((long) readInt16());
            case rdbEncInt32:
                return String.valueOf((long) readInt32());
            case rdbEncLZF:
                int inlen = readLength();
                int outlen = readLength();
                return new String(lzfDecompress(readBytes(inlen), outlen));
        }
    }

    public byte[] readStringBytes() {
        Tuple<Integer, Boolean> tuple = readEncodedLength();
        int length = tuple.getA();
        boolean encoded = tuple.getB();

        if (!encoded) {
            return readBytes(length);
        }
        int t = length;
        switch (t) {
            default:
                throw new RuntimeException(String.format("invalid encoded-string %02x", t));
            case rdbEncInt8:
                return String.valueOf((long) readInt8()).getBytes();
            case rdbEncInt16:
                return String.valueOf((long) readInt16()).getBytes();
            case rdbEncInt32:
                return String.valueOf((long) readInt32()).getBytes();
            case rdbEncLZF:
                int inlen = readLength();
                int outlen = readLength();
                return lzfDecompress(readBytes(inlen), outlen);
        }
    }


    private byte[] lzfDecompress(byte[] bytes, int outlen) {
        byte[] buf = new byte[outlen];
        LZFCompress.expand(bytes, 0, bytes.length, buf, 0, buf.length);
        return buf;
    }

    public Tuple<Integer, Boolean> readEncodedLength() {
        int u = (int) readUint8();

        int length = u & 0x3f;
        switch (u >> 6) {
            case rdb6bitLen:
                return Tuple.of(length, false);
            case rdb14bitLen:
                u = (int) readUint8();
                length = (length << 8) + u;
                return Tuple.of(length, false);
            case rdbEncVal:
                return Tuple.of(length, true);
            default:
                return Tuple.of(readUint32BigEndian(), false);
        }
    }

    public int readLength() {
        Tuple<Integer, Boolean> tuple = readEncodedLength();
        int length = tuple.getA();
        boolean encoded = tuple.getB();
        if (encoded) {
            throw new RuntimeException("encoded-length");
        }
        return length;
    }

    public int readUint32BigEndian() {
        byte[] b = new byte[4];
        readFull(b);
        return (b[3] & 0xff) | (b[2] & 0xff) << 8 | (b[1] & 0xff) << 16 | (b[0] & 0xff) << 24;
    }

    public byte[] readBytes(int n) {
        byte[] b = new byte[n];
        readFull(b);
        return b;
    }

    public int readByte() {
        byte[] buf  = new byte[1];
        readFull(buf);
        return buf[0];
    }

    private double readFloat64() {
        double val;
        int len = readByte();
        switch (len) {
            case 255:
                val = Double.NEGATIVE_INFINITY;
                return val;
            case 254:
                val = Double.POSITIVE_INFINITY;
                return val;
            case 253:
                val = Double.NaN;
                return val;
            default:
                return Double.parseDouble(new String(readBytes(len)));
        }
    }


    public long readUint8() {
        return LittleEndian.uint8(new byte[]{(byte) readByte()});
    }

    public long readUint16() {
        byte[] b = new byte[2];
        readFull(b);
        return LittleEndian.uint16(b);
    }

    public long readUint32() {
        byte[] b = new byte[4];
        readFull(b);
        return LittleEndian.uint32(b);
    }

    public long readUint64() {
        byte[] b = new byte[8];
        readFull(b);
        return LittleEndian.uint64(b);
    }


    public int readInt8() {
        byte[] b = new byte[1];
        readFull(b);
        return LittleEndian.int8(b);
    }

    public int readInt16() {
        byte[] b = new byte[2];
        readFull(b);
        return LittleEndian.int16(b);
    }

    public int readInt32() {
        byte[] b = new byte[4];
        readFull(b);
        return LittleEndian.int32(b);
    }

    public long readInt64() {
        byte[] b = new byte[8];
        readFull(b);
        return LittleEndian.int64(b);
    }




}
