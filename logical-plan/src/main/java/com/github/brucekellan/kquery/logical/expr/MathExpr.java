package com.github.brucekellan.kquery.logical.expr;

import com.github.brucekellan.kquery.logical.LogicalExpr;
import com.github.brucekellan.kquery.logical.LogicalPlan;
import com.github.brucekellan.kquery.schema.Field;
import lombok.Getter;

@Getter
public abstract class MathExpr implements LogicalExpr{

    private String name;

    private String op;

    private LogicalExpr l;

    private LogicalExpr r;

    public MathExpr(String name, String op, LogicalExpr l, LogicalExpr r) {
        this.name = name;
        this.op = op;
        this.l = l;
        this.r = r;
    }

    @Override
    public Field toField(LogicalPlan input) {
        return new Field(name, l.toField(input).getDataType());
    }
}
