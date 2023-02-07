package com.github.brucekellan.kquery.logical.expr;

import com.github.brucekellan.kquery.logical.LogicalExpr;

public class Avg extends AggregateExpr {

    private LogicalExpr expr;


    public Avg(LogicalExpr expr) {
        super("AVG", expr);
    }

}
