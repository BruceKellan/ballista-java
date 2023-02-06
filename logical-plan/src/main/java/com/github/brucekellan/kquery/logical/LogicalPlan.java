package com.github.brucekellan.kquery.logical;

import com.github.brucekellan.kquery.schema.Schema;

import java.util.List;

public interface LogicalPlan {

    Schema schema();

    List<LogicalPlan> children();

    default String pretty() {
        return LogicalPlanUtil.format(this, null);
    }

}
