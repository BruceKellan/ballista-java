package com.github.brucekellan.kquery.optimizer;

import com.github.brucekellan.kquery.logical.LogicalPlan;

public interface OptimizerRule {

    LogicalPlan optimize(LogicalPlan plan);

}
