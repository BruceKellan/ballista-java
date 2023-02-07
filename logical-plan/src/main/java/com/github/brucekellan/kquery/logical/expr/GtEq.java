package com.github.brucekellan.kquery.logical.expr;

import com.github.brucekellan.kquery.logical.LogicalExpr;

public class GtEq extends BooleanBinaryExpr {
    public GtEq(LogicalExpr l, LogicalExpr r) {
        super("gteq", ">=", l, r);
    }
}
