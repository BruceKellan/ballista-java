package com.github.brucekellan.kquery.logical.operator;

import com.github.brucekellan.kquery.logical.LogicalExpr;
import com.github.brucekellan.kquery.logical.LogicalPlan;
import com.github.brucekellan.kquery.schema.Schema;
import com.github.brucekellan.kquery.util.ListUtil;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.stream.Collectors;

public class Aggregate implements LogicalPlan {

    private LogicalPlan input;

    private List<LogicalExpr> groupExprs;

    private List<LogicalExpr> aggregateExprs;

    public Aggregate(LogicalPlan input, List<LogicalExpr> groupExprs, List<LogicalExpr> aggregateExprs) {
        this.input = input;
        this.groupExprs = groupExprs;
        this.aggregateExprs = aggregateExprs;
    }

    @Override
    public Schema schema() {
        return new Schema(ListUtil.concat(
                groupExprs.stream()
                        .map(groupExpr -> groupExpr.toField(input))
                        .collect(Collectors.toList()),
                aggregateExprs.stream()
                        .map(aggregateExpr -> aggregateExpr.toField(input))
                        .collect(Collectors.toList())));
    }

    @Override
    public List<LogicalPlan> children() {
        return Lists.newArrayList(input);
    }

    @Override
    public String toString() {
        return String.format("Aggregate: groupExprs=%s, aggregateExprs=%s", groupExprs, aggregateExprs);
    }
}
