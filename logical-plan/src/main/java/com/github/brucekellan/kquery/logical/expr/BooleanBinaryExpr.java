package com.github.brucekellan.kquery.logical.expr;

import com.github.brucekellan.kquery.datatypes.ArrowTypes;
import com.github.brucekellan.kquery.logical.LogicalExpr;
import com.github.brucekellan.kquery.logical.LogicalPlan;
import com.github.brucekellan.kquery.schema.Field;

public class BooleanBinaryExpr extends BinaryExpr{

    public BooleanBinaryExpr(String name, String op, LogicalExpr l, LogicalExpr r) {
        super(name, op, l, r);
    }

    @Override
    public Field toField(LogicalPlan input) {
        return new Field(getName(), ArrowTypes.BOOLEAN_TYPE);
    }
}
