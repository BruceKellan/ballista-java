package com.github.brucekellan.kquery.vector;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ColumnVectorTest {

    @Test
    public void testFieldColumnVector() {
        int size = 10;
        IntVector intVector = new IntVector("foo", new RootAllocator(Long.MAX_VALUE));
        intVector.allocateNew(size);
        intVector.setValueCount(size);
        FieldColumnVector.FieldColumnVectorBuilder builder = FieldColumnVector.builder();
        builder.fieldVector(intVector);
        builder.valueCount(size);
        for (int i = 0; i < size; i++) {
            builder.set(i, i);
        }
        FieldColumnVector fieldColumnVector = builder.build();
        Assertions.assertEquals(10, fieldColumnVector.getSize());
        for (int i = 0; i < size; i++) {
            Assertions.assertEquals(i, fieldColumnVector.getValue(i));
        }
    }

}
