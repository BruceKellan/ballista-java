package com.github.brucekellan.kquery.physical.expression;

public interface Accumulator {

    void accumulate(Object value);

    Object finalValue();

}
