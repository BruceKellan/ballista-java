package com.github.brucekellan.kquery.logical.expr;

import com.github.brucekellan.kquery.logical.LogicalExpr;

public class Multiply extends MathExpr {

    private LogicalExpr l;

    private LogicalExpr r;

    public Multiply(LogicalExpr l, LogicalExpr r) {
        super("mult", "*", l, r);
    }
}
