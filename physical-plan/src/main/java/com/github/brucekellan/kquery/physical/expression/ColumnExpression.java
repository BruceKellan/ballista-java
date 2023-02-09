package com.github.brucekellan.kquery.physical.expression;

import com.github.brucekellan.kquery.RecordBatch;
import com.github.brucekellan.kquery.vector.ColumnVector;

public class ColumnExpression implements Expression {

    private int i;

    public ColumnExpression(int i) {
        this.i = i;
    }

    @Override
    public ColumnVector evaluate(RecordBatch input) {
        return input.getField(i);
    }

    @Override
    public String toString() {
        return String.format("#%s", i);
    }
}
