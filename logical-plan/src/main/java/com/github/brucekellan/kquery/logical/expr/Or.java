package com.github.brucekellan.kquery.logical.expr;

import com.github.brucekellan.kquery.logical.LogicalExpr;

public class Or extends BooleanBinaryExpr {
    public Or(LogicalExpr l, LogicalExpr r) {
        super("or", "Or", l, r);
    }
}
