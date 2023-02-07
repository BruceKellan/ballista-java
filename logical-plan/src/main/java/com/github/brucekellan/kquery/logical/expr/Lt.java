package com.github.brucekellan.kquery.logical.expr;

import com.github.brucekellan.kquery.logical.LogicalExpr;

public class Lt extends BooleanBinaryExpr {
    public Lt(LogicalExpr l, LogicalExpr r) {
        super("lt", "<", l, r);
    }
}
