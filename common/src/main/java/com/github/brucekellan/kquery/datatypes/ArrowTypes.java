package com.github.brucekellan.kquery.datatypes;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;

public class ArrowTypes {

    public static final ArrowType.Bool BOOLEAN_TYPE = new ArrowType.Bool();

    public static final ArrowType.Int INT8_TYPE = new ArrowType.Int(8, true);

    public static final ArrowType.Int INT16_TYPE = new ArrowType.Int(16, true);

    public static final ArrowType.Int INT32_TYPE = new ArrowType.Int(32, true);

    public static final ArrowType.Int INT64_TYPE = new ArrowType.Int(64, true);

    public static final ArrowType.FloatingPoint FLOAT_TYPE = new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);

    public static final ArrowType.FloatingPoint DOUBLE_TYPE = new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);

    public static final ArrowType.Utf8 STRING_TYPE = new ArrowType.Utf8();

}
