package com.github.brucekellan.kquery.vector;

import com.github.brucekellan.kquery.datatypes.ArrowTypes;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

public class ArrowFieldVectorFactory {

    public static List<FieldVector> create(Schema schema, int valueCount) {
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, new RootAllocator(Long.MAX_VALUE));
        root.allocateNew();
        root.setRowCount(valueCount);
        return root.getFieldVectors();
    }

    public static FieldVector create(ArrowType arrowType, int initialCapacity) {
        if (arrowType == null) {
            throw new IllegalStateException();
        }
        FieldVector arrowFieldVector = null;
        RootAllocator rootAllocator = new RootAllocator(Long.MAX_VALUE);
        if (ArrowTypes.BOOLEAN_TYPE == arrowType) {
            arrowFieldVector = new BitVector("v", rootAllocator);
        } else if (ArrowTypes.INT8_TYPE == arrowType) {
            arrowFieldVector = new TinyIntVector("v", rootAllocator);
        } else if (ArrowTypes.INT16_TYPE == arrowType) {
            arrowFieldVector = new SmallIntVector("v", rootAllocator);
        } else if (ArrowTypes.INT32_TYPE == arrowType) {
            arrowFieldVector = new IntVector("v", rootAllocator);
        } else if (ArrowTypes.INT64_TYPE == arrowType) {
            arrowFieldVector = new BigIntVector("v", rootAllocator);
        } else if (ArrowTypes.FLOAT_TYPE == arrowType) {
            arrowFieldVector = new Float4Vector("v", rootAllocator);
        } else if (ArrowTypes.DOUBLE_TYPE == arrowType) {
            arrowFieldVector = new Float8Vector("v", rootAllocator);
        } else if (ArrowTypes.STRING_TYPE == arrowType) {
            arrowFieldVector = new VarCharVector("v", rootAllocator);
        } else {
            throw new IllegalStateException();
        }
        if (initialCapacity != 0) {
            arrowFieldVector.setInitialCapacity(initialCapacity);
        }
        arrowFieldVector.allocateNew();
        return arrowFieldVector;
    }

}
