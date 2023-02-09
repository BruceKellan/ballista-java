package com.github.brucekellan.kquery.physical.expression;

import com.github.brucekellan.kquery.RecordBatch;
import com.github.brucekellan.kquery.datatypes.ArrowTypes;
import com.github.brucekellan.kquery.vector.ArrowFieldVectorFactory;
import com.github.brucekellan.kquery.vector.ColumnVector;
import com.github.brucekellan.kquery.vector.FieldColumnVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.util.Objects;

public abstract class BooleanExpression implements Expression {

    private Expression l;

    private Expression r;

    @Override
    public ColumnVector evaluate(RecordBatch input) {
        ColumnVector ll = l.evaluate(input);
        ColumnVector rr = r.evaluate(input);
        assert ll.getSize() == rr.getSize();
        if (!Objects.equals(ll.getType(), rr.getType())) {
            throw new IllegalStateException(String.format("Binary expression operands do not have the same type: %s != %s", ll.getType().toString(), rr.getType().toString()));
        }
        return compare(ll, rr);
    }

    private ColumnVector compare(ColumnVector l, ColumnVector r) {
        int size = l.getSize();
        BitVector bitVector = (BitVector) ArrowFieldVectorFactory.create(ArrowTypes.BOOLEAN_TYPE, size);
        FieldColumnVector.FieldColumnVectorBuilder builder = FieldColumnVector.builder()
                .fieldVector(bitVector)
                .valueCount(size);
        for (int i = 0; i < size; i++) {
            Boolean value = evaluate(l.getValue(i), r.getValue(i), l.getType());
            builder.set(i, value);
        }
        return builder.build();
    }

    public abstract Boolean evaluate(Object ll, Object rr, ArrowType arrowType);
}
