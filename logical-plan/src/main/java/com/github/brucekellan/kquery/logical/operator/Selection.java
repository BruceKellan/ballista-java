package com.github.brucekellan.kquery.logical.operator;

import com.github.brucekellan.kquery.logical.LogicalExpr;
import com.github.brucekellan.kquery.logical.LogicalPlan;
import com.github.brucekellan.kquery.schema.Schema;
import com.google.common.collect.Lists;

import java.util.List;

public class Selection implements LogicalPlan{

    private LogicalPlan input;
og
    private LogicalExpr expr;

    public Selection(LogicalPlan input, LogicalExpr expr) {
        this.input = input;
        this.expr = expr;
    }

    @Override
    public Schema schema() {
        return input.schema();
    }

    @Override
    public List<LogicalPlan> children() {
        return Lists.newArrayList(input);
    }

    @Override
    public String toString() {
        return String.format("Selection: %s", expr);
    }
}
