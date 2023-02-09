package com.github.brucekellan.kquery.physical.expression;

public interface AggregateExpression {

    Expression inputExpression();

    Accumulator createAccumulator();

}
