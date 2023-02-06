package com.github.brucekellan.kquery.vector;

import com.github.brucekellan.kquery.datatypes.ArrowTypes;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.ArrowType;

public class FieldColumnVector implements ColumnVector {

    private FieldVector fieldVector;

    public FieldColumnVector(FieldVector fieldVector) {
        this.fieldVector = fieldVector;
    }

    @Override
    public ArrowType getType() {
        if (fieldVector instanceof BitVector) {
            return ArrowTypes.BOOLEAN_TYPE;
        } else if (fieldVector instanceof TinyIntVector) {
            return ArrowTypes.INT8_TYPE;
        } else if (fieldVector instanceof SmallIntVector) {
            return ArrowTypes.INT16_TYPE;
        } else if (fieldVector instanceof IntVector) {
            return ArrowTypes.INT32_TYPE;
        } else if (fieldVector instanceof BigIntVector) {
            return ArrowTypes.INT64_TYPE;
        } else if (fieldVector instanceof Float4Vector) {
            return ArrowTypes.FLOAT_TYPE;
        } else if (fieldVector instanceof Float8Vector) {
            return ArrowTypes.DOUBLE_TYPE;
        } else if (fieldVector instanceof VarCharVector) {
            return ArrowTypes.STRING_TYPE;
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    public Object getValue(int i) {
        if (fieldVector.isNull(i)) {
            return null;
        }
        if (fieldVector instanceof BitVector) {
            if (((BitVector) fieldVector).get(i) == 1) {
                return true;
            } else {
                return false;
            }
        } else if (fieldVector instanceof TinyIntVector) {
            return ((TinyIntVector) fieldVector).get(i);
        } else if (fieldVector instanceof SmallIntVector) {
            return ((SmallIntVector) fieldVector).get(i);
        } else if (fieldVector instanceof IntVector) {
            return ((IntVector) fieldVector).get(i);
        } else if (fieldVector instanceof BigIntVector) {
            return ((BigIntVector) fieldVector).get(i);
        } else if (fieldVector instanceof Float4Vector) {
            return ((Float4Vector) fieldVector).get(i);
        } else if (fieldVector instanceof Float8Vector) {
            return ((Float8Vector) fieldVector).get(i);
        } else if (fieldVector instanceof VarCharVector) {
            byte[] bytes = ((VarCharVector) fieldVector).get(i);
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
        return fieldVector.getValueCount();
    }

    public static FieldColumnVectorBuilder builder() {
        return new FieldColumnVectorBuilder();
    }

    public static class FieldColumnVectorBuilder {

        private FieldVector fieldVector;

        public FieldColumnVectorBuilder() {
        }

        public FieldColumnVectorBuilder fieldVector(FieldVector fieldVector) {
            this.fieldVector = fieldVector;
            return this;
        }

        public FieldColumnVectorBuilder valueCount(int n) {
            fieldVector.setValueCount(n);
            return this;
        }

        public FieldColumnVectorBuilder set(int i, Object value) {
            if (fieldVector instanceof BitVector) {
                if (value == null) {
                    ((BitVector) fieldVector).setNull(i);
                } else if (value instanceof Boolean) {
                    ((BitVector) fieldVector).set(i, (Boolean) value ? 1 : 0);
                } else if (value instanceof String) {
                    ((BitVector) fieldVector).set(i, Boolean.parseBoolean((String) value) ? 1 : 0);
                } else {
                    throw new IllegalStateException();
                }
            } else if (fieldVector instanceof TinyIntVector) {
                if (value == null) {
                    ((TinyIntVector) fieldVector).setNull(i);
                } else if (value instanceof Number) {
                    ((TinyIntVector) fieldVector).set(i, ((Number) value).byteValue());
                } else if (value instanceof String) {
                    ((TinyIntVector) fieldVector).set(i, Byte.parseByte((String) value));
                } else {
                    throw new IllegalStateException();
                }
            } else if (fieldVector instanceof SmallIntVector) {
                if (value == null) {
                    ((SmallIntVector) fieldVector).setNull(i);
                } else if (value instanceof Number) {
                    ((SmallIntVector) fieldVector).set(i, ((Number) value).shortValue());
                } else if (value instanceof String) {
                    ((SmallIntVector) fieldVector).set(i, Short.parseShort((String) value));
                } else {
                    throw new IllegalStateException();
                }
            } else if (fieldVector instanceof IntVector) {
                if (value == null) {
                    ((IntVector) fieldVector).setNull(i);
                } else if (value instanceof Number) {
                    ((IntVector) fieldVector).set(i, ((Number) value).intValue());
                } else if (value instanceof String) {
                    ((IntVector) fieldVector).set(i, Integer.parseInt((String) value));
                } else {
                    throw new IllegalStateException();
                }
            } else if (fieldVector instanceof BigIntVector) {
                if (value == null) {
                    ((BigIntVector) fieldVector).setNull(i);
                } else if (value instanceof Number) {
                    ((BigIntVector) fieldVector).set(i, ((Number) value).longValue());
                } else if (value instanceof String) {
                    ((BigIntVector) fieldVector).set(i, Long.parseLong((String) value));
                } else {
                    throw new IllegalStateException();
                }
            } else if (fieldVector instanceof Float4Vector) {
                if (value == null) {
                    ((Float4Vector) fieldVector).setNull(i);
                } else if (value instanceof Number) {
                    ((Float4Vector) fieldVector).set(i, ((Number) value).floatValue());
                } else if (value instanceof String) {
                    ((Float4Vector) fieldVector).set(i, Float.parseFloat((String) value));
                } else {
                    throw new IllegalStateException();
                }
            } else if (fieldVector instanceof Float8Vector) {
                if (value == null) {
                    ((Float8Vector) fieldVector).setNull(i);
                } else if (value instanceof Number) {
                    ((Float8Vector) fieldVector).set(i, ((Number) value).doubleValue());
                } else if (value instanceof String) {
                    ((Float8Vector) fieldVector).set(i, Double.parseDouble((String) value));
                } else {
                    throw new IllegalStateException();
                }
            } else if (fieldVector instanceof VarCharVector) {
                if (value == null) {
                    ((BitVector) fieldVector).setNull(i);
                } else if (value instanceof byte[]) {
                    ((VarCharVector) fieldVector).set(i, (byte[]) value);
                } else {
                    ((VarCharVector) fieldVector).set(i, value.toString().getBytes());
                }
            } else {
                throw new IllegalStateException();
            }
            return this;
        }

        public FieldColumnVector build() {
            return new FieldColumnVector(fieldVector);
        }
    }
}
