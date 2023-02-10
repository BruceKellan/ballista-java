package com.github.brucekellan.kquery.logical.expr;

import com.github.brucekellan.kquery.logical.LogicalExpr;
import com.github.brucekellan.kquery.logical.LogicalPlan;
import com.github.brucekellan.kquery.schema.Field;
import lombok.Getter;

@Getter
public class Alias implements LogicalExpr {

    private LogicalExpr expr;

    private String alias;

    public Alias(LogicalExpr expr, String alias) {
        this.expr = expr;
        this.alias = alias;
    }

    @Override
    public Field toField(LogicalPlan input) {
        return new Field(alias, expr.toField(input).getDataType());
    }

    @Override
    public String toString() {
        return String.format("%s as %s", expr, alias);
    }
}
