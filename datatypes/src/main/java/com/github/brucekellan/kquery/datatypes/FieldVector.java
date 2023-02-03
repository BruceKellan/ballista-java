package com.github.brucekellan.kquery.datatypes;

import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.ArrowType;

public class FieldVector implements ColumnVector {

    private org.apache.arrow.vector.FieldVector arrowFieldVector;

    public FieldVector(org.apache.arrow.vector.FieldVector arrowFieldVector) {
        this.arrowFieldVector = arrowFieldVector;
    }

    @Override
    public ArrowType getType() {
        if (arrowFieldVector instanceof BitVector) {
            return ArrowTypes.BOOLEAN_TYPE;
        } else if (arrowFieldVector instanceof TinyIntVector) {
            return ArrowTypes.INT8_TYPE;
        } else if (arrowFieldVector instanceof SmallIntVector) {
            return ArrowTypes.INT16_TYPE;
        } else if (arrowFieldVector instanceof IntVector) {
            return ArrowTypes.INT32_TYPE;
        } else if (arrowFieldVector instanceof BigIntVector) {
            return ArrowTypes.INT64_TYPE;
        } else if (arrowFieldVector instanceof Float4Vector) {
            return ArrowTypes.FLOAT_TYPE;
        } else if (arrowFieldVector instanceof Float8Vector) {
            return ArrowTypes.DOUBLE_TYPE;
        } else if (arrowFieldVector instanceof VarCharVector) {
            return ArrowTypes.STRING_TYPE;
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    public Object getValue(int i) {
        if (arrowFieldVector.isNull(i)) {
            return null;
        }
        if (arrowFieldVector instanceof BitVector) {
            if (((BitVector) arrowFieldVector).get(i) == 1) {
                return true;
            } else {
                return false;
            }
        } else if (arrowFieldVector instanceof TinyIntVector) {
            return ((TinyIntVector) arrowFieldVector).get(i);
        } else if (arrowFieldVector instanceof SmallIntVector) {
            return ((SmallIntVector) arrowFieldVector).get(i);
        } else if (arrowFieldVector instanceof IntVector) {
            return ((IntVector) arrowFieldVector).get(i);
        } else if (arrowFieldVector instanceof BigIntVector) {
            return ((BigIntVector) arrowFieldVector).get(i);
        } else if (arrowFieldVector instanceof Float4Vector) {
            return ((Float4Vector) arrowFieldVector).get(i);
        } else if (arrowFieldVector instanceof Float8Vector) {
            return ((Float8Vector) arrowFieldVector).get(i);
        } else if (arrowFieldVector instanceof VarCharVector) {
            byte[] bytes = ((VarCharVector) arrowFieldVector).get(i);
            if (bytes == null) {
                return null;
            } else {
                return new String(bytes);
            }
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    public int getSize() {
        return arrowFieldVector.getValueCount();
    }
}
