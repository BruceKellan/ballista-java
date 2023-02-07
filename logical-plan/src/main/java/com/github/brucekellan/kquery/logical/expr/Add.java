package com.github.brucekellan.kquery.logical.expr;

import com.github.brucekellan.kquery.logical.LogicalExpr;

public class Add extends MathExpr {

    private LogicalExpr l;

    private LogicalExpr r;

    public Add(LogicalExpr l, LogicalExpr r) {
        super("add", "+", l, r);
    }
}
