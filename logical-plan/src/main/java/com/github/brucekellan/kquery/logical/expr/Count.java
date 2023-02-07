package com.github.brucekellan.kquery.logical.expr;

import com.github.brucekellan.kquery.datatypes.ArrowTypes;
import com.github.brucekellan.kquery.logical.LogicalExpr;
import com.github.brucekellan.kquery.logical.LogicalPlan;
import com.github.brucekellan.kquery.schema.Field;

public class Count extends AggregateExpr {

    private LogicalExpr expr;

    public Count(LogicalExpr expr) {
        super("COUNT", expr);
    }

    @Override
    public Field toField(LogicalPlan input) {
        return new Field("COUNT", ArrowTypes.INT32_TYPE);
    }

    @Override
    public String toString() {
        return String.format("COUNT(%s)", expr);
    }
}
