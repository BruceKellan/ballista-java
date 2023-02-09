package com.github.brucekellan.kquery.physical.expression;

import com.github.brucekellan.kquery.RecordBatch;
import com.github.brucekellan.kquery.vector.ColumnVector;

import java.util.Objects;

public abstract class BinaryExpression implements Expression {

    private Expression l;

    private Expression r;

    public BinaryExpression(Expression l, Expression r) {
        this.l = l;
        this.r = r;
    }

    @Override
    public ColumnVector evaluate(RecordBatch input) {
        ColumnVector ll = l.evaluate(input);
        ColumnVector rr = r.evaluate(input);
        assert ll.getSize() == rr.getSize();
        if (!Objects.equals(ll.getType(), rr.getType())) {
            throw new IllegalStateException(String.format("Binary expression operands do not have the same type: %s != %s", ll.getType().toString(), rr.getType().toString()));
        }
        return evaluate(ll, rr);
    }

    public abstract ColumnVector evaluate(ColumnVector ll, ColumnVector rr);
}
