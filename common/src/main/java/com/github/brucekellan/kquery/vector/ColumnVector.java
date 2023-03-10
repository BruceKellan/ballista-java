package com.github.brucekellan.kquery.vector;

import org.apache.arrow.vector.types.pojo.ArrowType;

public interface ColumnVector {

    ArrowType getType();

    Object getValue(int i);

    int getSize();

}
