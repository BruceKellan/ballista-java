package com.github.brucekellan.kquery.logical.expr;

import com.github.brucekellan.kquery.logical.LogicalExpr;

public class Min extends AggregateExpr {

    private LogicalExpr expr;

    public Min(LogicalExpr expr) {
        super("MIN", expr);
    }

}
