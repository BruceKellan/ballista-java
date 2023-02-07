package com.github.brucekellan.kquery.logical.expr;

import com.github.brucekellan.kquery.logical.LogicalExpr;

public class Eq extends BooleanBinaryExpr {
    public Eq(LogicalExpr l, LogicalExpr r) {
        super("eq", "=", l, r);
    }
}
