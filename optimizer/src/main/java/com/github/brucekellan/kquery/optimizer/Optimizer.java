package com.github.brucekellan.kquery.optimizer;

import com.github.brucekellan.kquery.logical.LogicalPlan;
import com.google.common.collect.Lists;

import java.util.List;

public class Optimizer {

    private List<OptimizerRule> ruleList;

    public Optimizer() {
        ruleList = Lists.newArrayList(
                new ProjectionPushDownRule()
        );
    }

    public LogicalPlan optimize(LogicalPlan plan) {
        LogicalPlan output = plan;
        for (OptimizerRule optimizerRule : ruleList) {
            output = optimizerRule.optimize(output);
        }
        return output;
    }

}
