package com.github.brucekellan.kquery.logical.operator;

import com.github.brucekellan.kquery.logical.LogicalExpr;
import com.github.brucekellan.kquery.logical.LogicalPlan;
import com.github.brucekellan.kquery.schema.Schema;
import com.google.common.collect.Lists;
import lombok.Getter;

import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;

@Getter
public class Projection implements LogicalPlan {

    private LogicalPlan input;

    private List<LogicalExpr> exprs;

    public Projection(LogicalPlan input, List<LogicalExpr> exprs) {
        this.input = input;
        this.exprs = exprs;
    }

    @Override
    public Schema schema() {
        return new Schema(exprs.stream()
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
        for (LogicalExpr expression : exprs) {
            joiner.add(expression.toString());
        }
        return "Projection: " + joiner;
    }
}
