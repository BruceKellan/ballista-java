package com.github.brucekellan.kquery.logical.expr;

import com.github.brucekellan.kquery.logical.LogicalExpr;
import com.github.brucekellan.kquery.logical.LogicalPlan;
import com.github.brucekellan.kquery.schema.Field;

public abstract class AggregateExpr implements LogicalExpr {

    private String name;

    private LogicalExpr expr;

    public AggregateExpr(String name, LogicalExpr expr) {
        this.name = name;
        this.expr = expr;
    }

    @Override
    public Field toField(LogicalPlan input) {
        return new Field(name, expr.toField(input).getDataType());
    }

    @Override
    public String toString() {
        return String.format("%s(%s)", name, expr);
    }
}
