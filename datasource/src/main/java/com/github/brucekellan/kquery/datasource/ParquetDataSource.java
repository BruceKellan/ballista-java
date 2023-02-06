package com.github.brucekellan.kquery.datasource;

import com.github.brucekellan.kquery.RecordBatch;
import com.github.brucekellan.kquery.schema.Schema;
import org.apache.parquet.arrow.schema.SchemaConverter;

import java.util.Collection;
import java.util.List;

public class ParquetDataSource implements DataSource{

    private String filename;

    public ParquetDataSource(String filename) {
        this.filename = filename;
    }


    @Override
    public Schema schema() {
//        new SchemaConverter().fromParquet(it.schema).arrowSchema
        return null;
    }

    @Override
    public Collection<RecordBatch> scan(List<String> projection) {
        return null;
    }
}
