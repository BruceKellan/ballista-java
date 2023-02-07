package com.github.brucekellan.kquery.logical.expr;

import com.github.brucekellan.kquery.datatypes.ArrowTypes;
import com.github.brucekellan.kquery.logical.LogicalExpr;
import com.github.brucekellan.kquery.logical.LogicalPlan;
import com.github.brucekellan.kquery.schema.Field;

public class LiteralLong implements LogicalExpr {

    private Long n;

    public LiteralLong(Long n) {
        this.n = n;
    }

    @Override
    public Field toField(LogicalPlan input) {
        return new Field(n.toString(), ArrowTypes.INT64_TYPE);
    }

    @Override
    public String toString() {
        return n.toString();
    }
}
