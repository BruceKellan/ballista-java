package com.github.brucekellan.kquery.optimizer;

import com.github.brucekellan.kquery.logical.LogicalPlan;
import com.github.brucekellan.kquery.logical.expr.AggregateExpr;
import com.github.brucekellan.kquery.logical.operator.Aggregate;
import com.github.brucekellan.kquery.logical.operator.Projection;
import com.github.brucekellan.kquery.logical.operator.Scan;
import com.github.brucekellan.kquery.logical.operator.Selection;
import com.github.brucekellan.kquery.schema.Field;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ProjectionPushDownRule implements OptimizerRule {

    @Override
    public LogicalPlan optimize(LogicalPlan plan) {
        return pushDown(plan, Sets.newHashSet());
    }

    private LogicalPlan pushDown(LogicalPlan plan, Set<String> columnNames) {
        if (plan instanceof Projection) {
            OptimizerUtil.extractColumns(((Projection) plan).getExprs(), ((Projection) plan).getInput(), columnNames);
            LogicalPlan input = pushDown(((Projection) plan).getInput(), columnNames);
            return new Projection(input, ((Projection) plan).getExprs());
        } else if (plan instanceof Selection) {
            OptimizerUtil.extractColumns(((Selection) plan).getExpr(), ((Selection) plan).getInput(), columnNames);
            LogicalPlan input = pushDown(((Selection) plan).getInput(), columnNames);
            return new Selection(input, ((Selection) plan).getExpr());
        } else if (plan instanceof Aggregate) {
            OptimizerUtil.extractColumns(((Aggregate) plan).getGroupExprs(), ((Aggregate) plan).getInput(), columnNames);
            OptimizerUtil.extractColumns(((Aggregate) plan).getAggregateExprs().stream()
                            .map(AggregateExpr::getExpr)
                            .collect(Collectors.toList()),
                    ((Aggregate) plan).getInput(),
                    columnNames);
            LogicalPlan input = pushDown(((Aggregate) plan).getInput(), columnNames);
            return new Aggregate(input, ((Aggregate) plan).getGroupExprs(), ((Aggregate) plan).getAggregateExprs());
        } else if (plan instanceof Scan) {
            Set<String> validFieldName = ((Scan) plan).getDataSource().schema().getFields().stream()
                    .map(Field::getName)
                    .collect(Collectors.toSet());
            List<String> pushDownFieldNames = validFieldName.stream()
                    .filter(columnNames::contains)
                    .distinct().sorted().collect(Collectors.toList());
            return new Scan(((Scan) plan).getDataSource(), pushDownFieldNames);
        } else {
            throw new IllegalStateException(String.format("ProjectionPushDownRule does not support plan: %s", plan));
        }
    }
}
