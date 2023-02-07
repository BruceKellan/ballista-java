package com.github.brucekellan.kquery.logical.expr;

import com.github.brucekellan.kquery.datatypes.ArrowTypes;
import com.github.brucekellan.kquery.logical.LogicalExpr;
import com.github.brucekellan.kquery.logical.LogicalPlan;
import com.github.brucekellan.kquery.schema.Field;

public class Not extends UnaryExpr {

    public Not(LogicalExpr expr) {
        super("not", "NOT", expr);
    }

    @Override
    public Field toField(LogicalPlan input) {
        return new Field(getName(), ArrowTypes.BOOLEAN_TYPE);
    }
}
