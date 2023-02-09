package com.github.brucekellan.kquery.physical.expression;

import com.github.brucekellan.kquery.RecordBatch;
import com.github.brucekellan.kquery.vector.ColumnVector;

public interface Expression {

    ColumnVector evaluate(RecordBatch input);

}
