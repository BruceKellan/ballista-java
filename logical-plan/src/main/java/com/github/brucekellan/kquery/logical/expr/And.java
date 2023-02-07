package com.github.brucekellan.kquery.logical.expr;

import com.github.brucekellan.kquery.logical.LogicalExpr;

public class And extends BooleanBinaryExpr {
    public And(LogicalExpr l, LogicalExpr r) {
        super("and", "AND", l, r);
    }
}
