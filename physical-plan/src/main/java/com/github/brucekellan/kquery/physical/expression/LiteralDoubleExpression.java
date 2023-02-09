package com.github.brucekellan.kquery.physical.expression;

import com.github.brucekellan.kquery.RecordBatch;
import com.github.brucekellan.kquery.datatypes.ArrowTypes;
import com.github.brucekellan.kquery.vector.ColumnVector;
import com.github.brucekellan.kquery.vector.LiteralValueVector;

public class LiteralDoubleExpression implements Expression{

    private Double value;

    public LiteralDoubleExpression(Double value) {
        this.value = value;
    }

    @Override
    public ColumnVector evaluate(RecordBatch input) {
        return new LiteralValueVector(ArrowTypes.DOUBLE_TYPE, value, input.getRowCount());
    }
}
