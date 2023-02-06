package com.github.brucekellan.kquery.logical;

import com.github.brucekellan.kquery.schema.Field;

public interface LogicalExpression {

    Field toField(LogicalPlan input);

}
