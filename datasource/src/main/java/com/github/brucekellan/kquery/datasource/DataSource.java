package com.github.brucekellan.kquery.datasource;

import com.github.brucekellan.kquery.RecordBatch;
import com.github.brucekellan.kquery.schema.Schema;

import java.util.Collection;
import java.util.List;

public interface DataSource {

    Schema schema();

    // TODO: Maybe we can use Stream<RecordBatch>
    Iterable<RecordBatch> scan(List<String> projection);

}
