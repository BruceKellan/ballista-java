package com.github.brucekellan.kquery.logical;

import com.github.brucekellan.kquery.logical.expr.AggregateExpr;
import com.github.brucekellan.kquery.logical.operator.Aggregate;
import com.github.brucekellan.kquery.logical.operator.Projection;
import com.github.brucekellan.kquery.logical.operator.Selection;
import com.github.brucekellan.kquery.schema.Schema;

import java.util.List;

public class DataFrameImpl implements DataFrame {

    private LogicalPlan plan;

    public DataFrameImpl(LogicalPlan plan) {
        this.plan = plan;
    }

    @Override
    public DataFrame project(List<LogicalExpr> exprs) {
        return new DataFrameImpl(new Projection(plan, exprs));
    }

    @Override
    public DataFrame filter(LogicalExpr expr) {
        return new DataFrameImpl(new Selection(plan, expr));
    }

    @Override
    public DataFrame aggregate(List<LogicalExpr> groupByExprs, List<AggregateExpr> aggregateExprs) {
        return new DataFrameImpl(new Aggregate(plan, groupByExprs, aggregateExprs));
    }

    @Override
    public Schema schema() {
        return plan.schema();
    }

    @Override
    public LogicalPlan logicalPlan() {
        return plan;
    }
}
