package com.github.brucekellan.kquery.logical.expr;

import com.github.brucekellan.kquery.logical.LogicalExpr;

public class Max extends AggregateExpr {

    private LogicalExpr expr;


    public Max(LogicalExpr expr) {
        super("MAX", expr);
    }

}
