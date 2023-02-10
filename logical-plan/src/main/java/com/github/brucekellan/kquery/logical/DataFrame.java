package com.github.brucekellan.kquery.logical;

import com.github.brucekellan.kquery.logical.expr.AggregateExpr;
import com.github.brucekellan.kquery.schema.Schema;

import java.util.List;

public interface DataFrame {

    DataFrame project(List<LogicalExpr> exprs);

    DataFrame filter(LogicalExpr expr);

    DataFrame aggregate(List<LogicalExpr> groupByExprs, List<AggregateExpr> aggregateExprs);

    Schema schema();

    LogicalPlan logicalPlan();

}
