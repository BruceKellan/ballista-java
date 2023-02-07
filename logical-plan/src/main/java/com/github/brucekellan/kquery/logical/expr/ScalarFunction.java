package com.github.brucekellan.kquery.logical.expr;

import com.github.brucekellan.kquery.logical.LogicalExpr;
import com.github.brucekellan.kquery.logical.LogicalPlan;
import com.github.brucekellan.kquery.schema.Field;
import org.apache.arrow.vector.types.pojo.ArrowType;

import java.util.List;

public class ScalarFunction implements LogicalExpr {

    private String name;

    private List<LogicalExpr> args;

    private ArrowType returnType;

    @Override
    public Field toField(LogicalPlan input) {
        return new Field(name, returnType);
    }

    @Override
    public String toString() {
        return String.format("%s(%s)", name, args);
    }
}
