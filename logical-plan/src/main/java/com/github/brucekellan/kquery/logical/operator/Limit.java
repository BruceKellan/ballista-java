package com.github.brucekellan.kquery.logical.operator;

import com.github.brucekellan.kquery.logical.LogicalPlan;
import com.github.brucekellan.kquery.schema.Schema;
import com.google.common.collect.Lists;

import java.util.List;

public class Limit implements LogicalPlan {

    private LogicalPlan input;

    private int limit;

    public Limit(LogicalPlan input, int limit) {
        this.input = input;
        this.limit = limit;
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
        return "Limit: " + limit;
    }
}
