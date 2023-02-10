package com.github.brucekellan.kquery.planner;

import com.github.brucekellan.kquery.logical.LogicalExpr;
import com.github.brucekellan.kquery.logical.LogicalPlan;
import com.github.brucekellan.kquery.logical.expr.Column;
import com.github.brucekellan.kquery.logical.expr.LiteralLong;
import com.github.brucekellan.kquery.logical.operator.Scan;
import com.github.brucekellan.kquery.logical.operator.Selection;
import com.github.brucekellan.kquery.physical.PhysicalPlan;
import com.github.brucekellan.kquery.physical.exec.ScanExec;
import com.github.brucekellan.kquery.physical.exec.SelectionExec;
import com.github.brucekellan.kquery.physical.expression.Expression;
import com.github.brucekellan.kquery.physical.expression.LiteralLongExpression;

public class QueryPlanner {

    public PhysicalPlan createPhysicalPlan(LogicalPlan plan) {
        PhysicalPlan physicalPlan = null;
        if (plan instanceof Scan) {
            return new ScanExec(((Scan) plan).getDataSource(), ((Scan) plan).getProjection());
        } else if (plan instanceof Selection) {
            PhysicalPlan input = createPhysicalPlan(((Selection) plan).getInput());
            Expression physicalExpr = createPhysicalExpr(((Selection) plan).getExpr(), ((Selection) plan).getInput());
            return new SelectionExec(input, physicalExpr);
        } else {
            return null;
        }
    }

    public Expression createPhysicalExpr(LogicalExpr expr, LogicalPlan logicalPlan) {
        if (expr instanceof LiteralLong) {
            return new LiteralLongExpression(((LiteralLong) expr).getN());
        }
        return null;
    }

}
