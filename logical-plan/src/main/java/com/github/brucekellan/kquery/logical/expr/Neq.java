package com.github.brucekellan.kquery.logical.expr;

import com.github.brucekellan.kquery.logical.LogicalExpr;

public class Neq extends BooleanBinaryExpr {
    public Neq(LogicalExpr l, LogicalExpr r) {
        super("neq", "!=", l, r);
    }
}
