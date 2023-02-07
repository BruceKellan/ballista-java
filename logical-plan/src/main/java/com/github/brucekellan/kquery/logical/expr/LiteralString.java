package com.github.brucekellan.kquery.logical.expr;

import com.github.brucekellan.kquery.datatypes.ArrowTypes;
import com.github.brucekellan.kquery.logical.LogicalExpr;
import com.github.brucekellan.kquery.logical.LogicalPlan;
import com.github.brucekellan.kquery.schema.Field;

public class LiteralString implements LogicalExpr {

    private String str;

    public LiteralString(String str) {
        this.str = str;
    }

    @Override
    public Field toField(LogicalPlan input) {
        return new Field(str, ArrowTypes.STRING_TYPE);
    }

    @Override
    public String toString() {
        return String.format("'%s'", str);
    }
}
