package org.chengxiang.bloomrf;

public class MapperUtil {
    private static LongMapper longMapper = new LongMapper();
    private static IntegerMapper intMapper = new IntegerMapper();
    private static DoubleMapper doubleMapper = new DoubleMapper();
    private static FloatMapper floatMapper = new FloatMapper();

    public static UnsignedLong toUnsignedLong(long value) {
        return longMapper.toULong(value);
    }

    public static UnsignedLong toUnsignedLong(int value) {
        return intMapper.toULong(value);
    }

    public static UnsignedLong toUnsignedLong(double value) {
        return doubleMapper.toULong(value);
    }

    public static UnsignedLong toUnsignedLong(float value) {
        return floatMapper.toULong(value);
    }
}
