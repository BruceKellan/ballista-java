package com.github.brucekellan.kquery.optimizer;

import com.github.brucekellan.kquery.logical.LogicalExpr;
import com.github.brucekellan.kquery.logical.LogicalPlan;
import com.github.brucekellan.kquery.logical.expr.*;

import java.util.List;
import java.util.Set;

public class OptimizerUtil {

    public static void extractColumns(List<LogicalExpr> exprs, LogicalPlan input, Set<String> accums) {
        for (LogicalExpr expr : exprs) {
            OptimizerUtil.extractColumns(expr, input, accums);
        }
    }

    public static void extractColumns(LogicalExpr expr, LogicalPlan input, Set<String> accums) {
        if (expr instanceof ColumnIndex) {
            accums.add(input.schema().getFields().get(((ColumnIndex) expr).getI()).getName());
        } else if (expr instanceof Column) {
            accums.add(((Column) expr).getName());
        } else if (expr instanceof BinaryExpr) {
            extractColumns(((BinaryExpr) expr).getL(), input, accums);
            extractColumns(((BinaryExpr) expr).getR(), input, accums);
        } else if (expr instanceof Alias) {
            extractColumns(((Alias) expr).getExpr(), input, accums);
        } else if (expr instanceof CastExpr) {
            extractColumns(((CastExpr) expr).getExpr(), input, accums);
        } else if (expr instanceof LiteralString) {

        } else if (expr instanceof LiteralLong) {

        } else if (expr instanceof LiteralDouble) {

        } else {
            throw new IllegalStateException(String.format("extractColumns does not support expression: ", expr));
        }
    }

}
