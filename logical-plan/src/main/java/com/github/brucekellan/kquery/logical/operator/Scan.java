package com.github.brucekellan.kquery.logical.operator;

import com.github.brucekellan.kquery.datasource.DataSource;
import com.github.brucekellan.kquery.logical.LogicalPlan;
import com.github.brucekellan.kquery.schema.Schema;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;

public class Scan implements LogicalPlan {

    private String path;

    private DataSource dataSource;

    private List<String> projection;

    private Schema schema;

    public Scan(String path, DataSource dataSource, List<String> projection) {
        this.path = path;
        this.dataSource = dataSource;
        this.projection = projection;
        this.schema = deriveSchema();
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public List<LogicalPlan> children() {
        return Lists.newArrayList();
    }

    private Schema deriveSchema() {
        Schema schema = dataSource.schema();
        if (CollectionUtils.isEmpty(projection)) {
            return schema;
        } else {
            return schema.select(projection);
        }
    }

    @Override
    public String toString() {
        if (CollectionUtils.isEmpty(projection)) {
            return "Scan: " + path + "; projection=None";
        } else {
            return "Scan: " + path + "; projection=" + projection.toString();
        }
    }
}
