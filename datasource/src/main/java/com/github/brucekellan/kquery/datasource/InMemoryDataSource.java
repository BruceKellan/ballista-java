package com.github.brucekellan.kquery.datasource;

import com.github.brucekellan.kquery.RecordBatch;
import com.github.brucekellan.kquery.schema.Schema;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

class InMemoryDataSource implements DataSource {

    private Schema schema;

    private List<RecordBatch> data;

    public InMemoryDataSource(Schema schema, List<RecordBatch> data) {
        this.schema = schema;
        this.data = data;
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Collection<RecordBatch> scan(List<String> projection) {
        List<Integer> projectionIndices = projection.stream()
                .map(name -> schema.getFields().indexOf(name))
                .collect(Collectors.toList());
        return data.stream()
                .map(batch -> new RecordBatch(schema, projectionIndices
                        .stream()
                        .map(batch::getField)
                        .collect(Collectors.toList())
                )).collect(Collectors.toList());
    }

}
