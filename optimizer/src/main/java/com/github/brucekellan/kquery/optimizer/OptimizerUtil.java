package com.github.brucekellan.kquery.optimizer;

import com.github.brucekellan.kquery.logical.LogicalExpr;
import com.github.brucekellan.kquery.logical.LogicalPlan;
import com.github.brucekellan.kquery.logical.expr.*;

import java.util.List;
import java.util.Set;

public class OptimizerUtil {

    public static void extractColumns(List<LogicalExpr> exprs, LogicalPlan input, Set<String> columnNameAccums) {
        for (LogicalExpr expr : exprs) {
            OptimizerUtil.extractColumns(expr, input, columnNameAccums);
        }
    }

    /**
     * extract all column name from logic expression.
     * @param expr
     * @param input
     * @param columnNameAccums
     */
    public static void extractColumns(LogicalExpr expr, LogicalPlan input, Set<String> columnNameAccums) {
        if (expr instanceof ColumnIndex) {
            columnNameAccums.add(input.schema().getFields().get(((ColumnIndex) expr).getI()).getName());
        } else if (expr instanceof Column) {
            columnNameAccums.add(((Column) expr).getName());
        } else if (expr instanceof BinaryExpr) {
            extractColumns(((BinaryExpr) expr).getL(), input, columnNameAccums);
            extractColumns(((BinaryExpr) expr).getR(), input, columnNameAccums);
        } else if (expr instanceof Alias) {
            extractColumns(((Alias) expr).getExpr(), input, columnNameAccums);
        } else if (expr instanceof CastExpr) {
            extractColumns(((CastExpr) expr).getExpr(), input, columnNameAccums);
        } else if (expr instanceof LiteralString) {

        } else if (expr instanceof LiteralLong) {

        } else if (expr instanceof LiteralDouble) {

        } else {
            throw new IllegalStateException(String.format("extractColumns does not support expression: ", expr));
        }
    }

}
