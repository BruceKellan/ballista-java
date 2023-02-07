package com.github.brucekellan.kquery.logical.expr;

import com.github.brucekellan.kquery.logical.LogicalExpr;

public class Gt extends BooleanBinaryExpr {
    public Gt(LogicalExpr l, LogicalExpr r) {
        super("gt", ">", l, r);
    }
}
