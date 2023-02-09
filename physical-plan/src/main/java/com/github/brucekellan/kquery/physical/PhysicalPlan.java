package com.github.brucekellan.kquery.physical;

import com.github.brucekellan.kquery.RecordBatch;
import com.github.brucekellan.kquery.schema.Schema;

import java.util.List;

public interface PhysicalPlan {

    Schema schema();

    Iterable<RecordBatch> execute();

    List<PhysicalPlan> children();

    default String pretty() {
        return format(this);
    }

    static String format(PhysicalPlan plan) {
        return format(plan, 0);
    }

    static String format(PhysicalPlan plan, Integer indent) {
        if (indent == null) {
            indent = 0;
        }
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < indent; i++) {
            builder.append("\t");
        }
        builder.append(plan.toString()).append("\n");
        for (PhysicalPlan child : plan.children()) {
            builder.append(format(child, indent + 1));
        }
        return builder.toString();
    }
}
