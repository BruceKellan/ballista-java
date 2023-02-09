package com.github.brucekellan.kquery.physical.expression;

import com.github.brucekellan.kquery.RecordBatch;
import com.github.brucekellan.kquery.datatypes.ArrowTypes;
import com.github.brucekellan.kquery.vector.ColumnVector;
import com.github.brucekellan.kquery.vector.LiteralValueVector;

public class LiteralStringExpression implements Expression{

    private String value;

    public LiteralStringExpression(String value) {
        this.value = value;
    }

    @Override
    public ColumnVector evaluate(RecordBatch input) {
        return new LiteralValueVector(ArrowTypes.STRING_TYPE, value.getBytes(), input.getRowCount());
    }
}
