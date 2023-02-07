package com.github.brucekellan.kquery.logical.expr;

import com.github.brucekellan.kquery.logical.LogicalExpr;
import com.github.brucekellan.kquery.logical.LogicalPlan;
import com.github.brucekellan.kquery.schema.Field;
import org.apache.arrow.vector.types.pojo.ArrowType;

public class CastExpr implements LogicalExpr {

    private LogicalExpr expr;

    private ArrowType dataType;

    public CastExpr(LogicalExpr expr, ArrowType dataType) {
        this.expr = expr;
        this.dataType = dataType;
    }

    @Override
    public Field toField(LogicalPlan input) {
        return new Field(expr.toField(input).getName(), dataType);
    }

    @Override
    public String toString() {
        return String.format("CAST(%s as %s)", expr, dataType);
    }
}
