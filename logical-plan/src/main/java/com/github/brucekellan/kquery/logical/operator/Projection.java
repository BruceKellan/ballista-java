package com.github.brucekellan.kquery.logical.operator;

import com.github.brucekellan.kquery.logical.LogicalExpr;
import com.github.brucekellan.kquery.logical.LogicalPlan;
import com.github.brucekellan.kquery.schema.Schema;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;

public class Projection implements LogicalPlan {

    private LogicalPlan input;

    private List<LogicalExpr> expressions;

    public Projection(LogicalPlan input, List<LogicalExpr> expressions) {
        this.input = input;
        this.expressions = expressions;
    }

    @Override
    public Schema schema() {
        return new Schema(expressions.stream()
                .map(expression -> expression.toField(input))
                .collect(Collectors.toList()));
    }

    @Override
    public List<LogicalPlan> children() {
        return Lists.newArrayList(input);
    }

    @Override
    public String toString() {
        StringJoiner joiner = new StringJoiner(", ");
        for (LogicalExpr expression : expressions) {
            joiner.add(expression.toString());
        }
        return "Projection: " + joiner;
    }
}
