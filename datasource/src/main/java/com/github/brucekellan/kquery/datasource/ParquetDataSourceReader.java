package com.github.brucekellan.kquery.datasource;

import com.github.brucekellan.kquery.RecordBatch;
import com.github.brucekellan.kquery.vector.FieldColumnVector;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.arrow.schema.SchemaConverter;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.stream.Collectors;

public class ParquetDataSourceReader implements AutoCloseable, Iterable<RecordBatch> {

    private String filename;

    private List<String> columns;

    private ParquetFileReader reader = null;

    public ParquetDataSourceReader(String filename, List<String> columns) {
        this.filename = filename;
        this.columns = columns;
        try {
            this.reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(filename), new Configuration()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        this.reader.close();
    }

    @Override
    public Iterator<RecordBatch> iterator() {
        return new ParquetDataSourceReaderIterator(filename, columns);
    }

    public class ParquetDataSourceReaderIterator implements Iterator<RecordBatch>{

        private String filename;

        private List<String> projectedColumns;

        private MessageType schema = reader.getFooter().getFileMetaData().getSchema();

        private Schema arrowSchema = new SchemaConverter().fromParquet(schema).getArrowSchema();

        private Schema projectedArrowSchema = new Schema(projectedColumns.stream()
                .map(name -> arrowSchema.getFields().stream()
                        .filter(field -> Objects.equals(field.getName(), name))
                        .findFirst()
                        .orElseThrow(() -> new NoSuchElementException("Cannot found column " + name)))
                .collect(Collectors.toList()));

        private RecordBatch batch = null;

        public ParquetDataSourceReaderIterator(String filename, List<String> projectedColumns) {
            this.filename = filename;
            this.projectedColumns = projectedColumns;
        }

        @Override
        public boolean hasNext() {
            batch = nextBatch();
            return batch != null;
        }

        @Override
        public RecordBatch next() {
            RecordBatch next = batch;
            batch = null;
            if (next == null) {
                throw new IllegalStateException();
            }
            return next;
        }

        private RecordBatch nextBatch() {
            try {
                PageReadStore pages = reader.readNextRowGroup();
                if (pages == null) {
                    return null;
                }
                if (pages.getRowCount() > Integer.MAX_VALUE) {
                    throw new IllegalStateException();
                }
                int rows = new Long(pages.getRowCount()).intValue();
                System.out.println("Reading "+ rows + " rows");
                VectorSchemaRoot root = VectorSchemaRoot.create(projectedArrowSchema, new RootAllocator(Long.MAX_VALUE));
                root.allocateNew();
                root.setRowCount(rows);
                com.github.brucekellan.kquery.schema.Schema kQuerySchema = com.github.brucekellan.kquery.schema.SchemaConverter.fromArrowSchema(arrowSchema);
                batch = new RecordBatch(kQuerySchema, root.getFieldVectors().stream()
                        .map(FieldColumnVector::new)
                        .collect(Collectors.toList()));
                // TODO we really want to read directly as columns not rows
                //      val columnIO = ColumnIOFactory().getColumnIO(schema)
                //      val recordReader: RecordReader<Group> = columnIO.getRecordReader(pages, GroupRecordConverter(schema))
                return batch;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
