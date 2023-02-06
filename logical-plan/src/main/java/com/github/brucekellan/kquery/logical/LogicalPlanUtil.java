package com.github.brucekellan.kquery.logical;

public class LogicalPlanUtil {

    public static String format(LogicalPlan plan, Integer indent) {
        if (indent == null) {
            indent = 0;
        }
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < indent; i++) {
            builder.append("\t");
        }
        builder.append(plan.toString());
        for (LogicalPlan child : plan.children()) {
            builder.append(LogicalPlanUtil.format(child, indent + 1));
        }
        return builder.toString();
    }

}
