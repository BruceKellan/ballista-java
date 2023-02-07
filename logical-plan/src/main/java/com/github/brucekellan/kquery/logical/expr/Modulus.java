package com.github.brucekellan.kquery.logical.expr;

import com.github.brucekellan.kquery.logical.LogicalExpr;

public class Modulus extends MathExpr {

    private LogicalExpr l;

    private LogicalExpr r;

    public Modulus(LogicalExpr l, LogicalExpr r) {
        super("mod", "%", l, r);
    }
}
