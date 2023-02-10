package com.github.brucekellan.kquery.logical.expr;

import com.github.brucekellan.kquery.logical.LogicalExpr;
import com.github.brucekellan.kquery.logical.LogicalPlan;
import com.github.brucekellan.kquery.schema.Field;
import lombok.Getter;

@Getter
public class ColumnIndex implements LogicalExpr {

    private int i;

    public ColumnIndex(int i) {
        this.i = i;
    }

    @Override
    public Field toField(LogicalPlan input) {
        return input.schema().getFields().get(i);
    }

    @Override
    public String toString() {
        return String.format("#%s", i);
    }
}
