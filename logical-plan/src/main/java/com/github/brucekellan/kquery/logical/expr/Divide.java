package com.github.brucekellan.kquery.logical.expr;

import com.github.brucekellan.kquery.logical.LogicalExpr;

public class Divide extends MathExpr {

    private LogicalExpr l;

    private LogicalExpr r;

    public Divide(LogicalExpr l, LogicalExpr r) {
        super("div", "/", l, r);
    }
}
