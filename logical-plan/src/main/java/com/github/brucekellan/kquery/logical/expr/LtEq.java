package com.github.brucekellan.kquery.logical.expr;

import com.github.brucekellan.kquery.logical.LogicalExpr;

public class LtEq extends BooleanBinaryExpr {
    public LtEq(LogicalExpr l, LogicalExpr r) {
        super("lteq", "<=", l, r);
    }
}
