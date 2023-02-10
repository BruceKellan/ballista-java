package com.github.brucekellan.kquery.execution;

import com.github.brucekellan.kquery.RecordBatch;
import com.github.brucekellan.kquery.datasource.CsvDataSource;
import com.github.brucekellan.kquery.execution.configuration.BallistaConfig;
import com.github.brucekellan.kquery.logical.DataFrame;
import com.github.brucekellan.kquery.logical.DataFrameImpl;
import com.github.brucekellan.kquery.logical.LogicalPlan;
import com.github.brucekellan.kquery.logical.operator.Scan;
import com.github.brucekellan.kquery.optimizer.Optimizer;
import com.github.brucekellan.kquery.physical.PhysicalPlan;
import com.github.brucekellan.kquery.planner.QueryPlanner;
import com.google.common.collect.Lists;
import lombok.Builder;

import java.util.Map;

public class ExecutionContext {

    private Map<String, String> settings;

    @Builder
    public ExecutionContext(Map<String, String> settings) {
        this.settings = settings;
    }

    public DataFrame csv(String filePath) {
        int batchSize = Integer.parseInt(settings.getOrDefault(BallistaConfig.CSV_BATCH_SIZE, "1024"));
        return new DataFrameImpl(new Scan(new CsvDataSource(filePath, null, true, batchSize), Lists.newArrayList()));
    }

    public Iterable<RecordBatch> execute(DataFrame dataFrame) {
        return execute(dataFrame.logicalPlan());
    }

    public Iterable<RecordBatch> execute(LogicalPlan plan) {
        plan = new Optimizer().optimize(plan);
        PhysicalPlan physicalPlan = new QueryPlanner().createPhysicalPlan(plan);
        return physicalPlan.execute();
    }
}
