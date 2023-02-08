package com.github.brucekellan.kquery.logical;

import com.github.brucekellan.kquery.schema.Schema;

import java.util.List;

public interface LogicalPlan {

    Schema schema();

    List<LogicalPlan> children();

    default String pretty() {
        return format(this);
    }

    static String format(LogicalPlan plan) {
        return format(plan, 0);
    }

    static String format(LogicalPlan plan, Integer indent) {
        if (indent == null) {
            indent = 0;
        }
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < indent; i++) {
            builder.append("\t");
        }
        builder.append(plan.toString()).append("\n");
        for (LogicalPlan child : plan.children()) {
            builder.append(format(child, indent + 1));
        }
        return builder.toString();
    }
}
