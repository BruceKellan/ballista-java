package com.github.brucekellan.kquery.logical.expr;

import com.github.brucekellan.kquery.logical.LogicalExpr;

public class Sum extends AggregateExpr {

    private LogicalExpr expr;


    public Sum(LogicalExpr expr) {
        super("SUM", expr);
    }

}
