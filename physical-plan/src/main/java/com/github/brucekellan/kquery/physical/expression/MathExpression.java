package com.github.brucekellan.kquery.physical.expression;

import com.github.brucekellan.kquery.vector.ArrowFieldVectorFactory;
import com.github.brucekellan.kquery.vector.ColumnVector;
import com.github.brucekellan.kquery.vector.FieldColumnVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.ArrowType;

public abstract class MathExpression extends BinaryExpression{

    public MathExpression(Expression l, Expression r) {
        super(l, r);
    }

    @Override
    public ColumnVector evaluate(ColumnVector ll, ColumnVector rr) {
        int size = ll.getSize();
        FieldVector fieldVector = ArrowFieldVectorFactory.create(ll.getType(), size);
        FieldColumnVector.FieldColumnVectorBuilder builder = FieldColumnVector.builder()
                .fieldVector(fieldVector)
                .valueCount(size);
        for (int i = 0; i < size; i++) {
            builder.set(i, evaluate(ll.getValue(i), rr.getValue(i), ll.getType()));
        }
        return builder.build();
    }

    public abstract Object evaluate(Object l, Object r, ArrowType arrowType);

}
