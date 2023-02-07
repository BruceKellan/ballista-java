package com.github.brucekellan.kquery.logical.expr;

import com.github.brucekellan.kquery.logical.LogicalExpr;
import lombok.Getter;

@Getter
public abstract class UnaryExpr implements LogicalExpr {

    private String name;

    private String op;

    private LogicalExpr expr;

    public UnaryExpr(String name, String op, LogicalExpr expr) {
        this.name = name;
        this.op = op;
        this.expr = expr;
    }

    @Override
    public String toString() {
        return String.format("%s %s", op, expr);
    }
}
