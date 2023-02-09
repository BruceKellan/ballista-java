package com.github.brucekellan.kquery.physical.exec;

import com.github.brucekellan.kquery.RecordBatch;
import com.github.brucekellan.kquery.datasource.DataSource;
import com.github.brucekellan.kquery.physical.PhysicalPlan;
import com.github.brucekellan.kquery.schema.Schema;
import com.google.common.collect.Lists;

import java.util.List;

public class ScanExec implements PhysicalPlan {

    private DataSource dataSource;

    private List<String> projection;

    @Override
    public Schema schema() {
        return dataSource.schema().select(projection);
    }

    @Override
    public Iterable<RecordBatch> execute() {
        return dataSource.scan(projection);
    }

    @Override
    public List<PhysicalPlan> children() {
        return Lists.newArrayList();
    }
}
