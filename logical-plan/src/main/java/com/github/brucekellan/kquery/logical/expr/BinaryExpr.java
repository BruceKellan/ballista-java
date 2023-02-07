package com.github.brucekellan.kquery.logical.expr;

import com.github.brucekellan.kquery.logical.LogicalExpr;
import lombok.Getter;

@Getter
public abstract class BinaryExpr implements LogicalExpr {

    private String name;

    private String op;

    private LogicalExpr l;

    private LogicalExpr r;

    public BinaryExpr(String name, String op, LogicalExpr l, LogicalExpr r) {
        this.name = name;
        this.op = op;
        this.l = l;
        this.r = r;
    }

    @Override
    public String toString() {
        return String.format("%s %s %s", l, op, r);
    }
}
