package com.github.brucekellan.kquery.physical.expression;

import com.github.brucekellan.kquery.datatypes.ArrowTypes;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.util.Objects;

public class NeqExpression extends BooleanExpression {

    @Override
    public Boolean evaluate(Object ll, Object rr, ArrowType arrowType) {
        if (arrowType != ArrowTypes.BOOLEAN_TYPE &&
                arrowType != ArrowTypes.INT8_TYPE &&
                arrowType != ArrowTypes.INT16_TYPE &&
                arrowType != ArrowTypes.INT32_TYPE &&
                arrowType != ArrowTypes.INT64_TYPE &&
                arrowType != ArrowTypes.FLOAT_TYPE &&
                arrowType != ArrowTypes.DOUBLE_TYPE &&
                arrowType != ArrowTypes.STRING_TYPE) {
            throw new IllegalStateException("Unsupported data type in comparison expression: " + arrowType);
        }
        return !Objects.equals(ll, rr);
    }
}
