package com.github.brucekellan.kquery.physical.expression;

import com.github.brucekellan.kquery.RecordBatch;
import com.github.brucekellan.kquery.datatypes.ArrowTypes;
import com.github.brucekellan.kquery.vector.ColumnVector;
import com.github.brucekellan.kquery.vector.LiteralValueVector;

public class LiteralLongExpression implements Expression{

    private Long value;

    public LiteralLongExpression(Long value) {
        this.value = value;
    }

    @Override
    public ColumnVector evaluate(RecordBatch input) {
        return new LiteralValueVector(ArrowTypes.INT64_TYPE, value, input.getRowCount());
    }
}
