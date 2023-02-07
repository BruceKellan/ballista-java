package com.github.brucekellan.kquery.logical.expr;

import com.github.brucekellan.kquery.logical.LogicalExpr;

public class Subtract extends MathExpr {

    private LogicalExpr l;

    private LogicalExpr r;

    public Subtract(LogicalExpr l, LogicalExpr r) {
        super("subtract", "-", l, r);
    }
}
