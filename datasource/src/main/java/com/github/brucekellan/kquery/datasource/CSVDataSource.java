package com.github.brucekellan.kquery.datasource;

import com.github.brucekellan.kquery.RecordBatch;
import com.github.brucekellan.kquery.datatypes.ArrowTypes;
import com.github.brucekellan.kquery.schema.Field;
import com.github.brucekellan.kquery.schema.Schema;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.apache.commons.collections.CollectionUtils;

import java.io.*;
import java.nio.file.Files;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class CSVDataSource implements DataSource {

    private String filename;

    private Schema schema;

    private boolean hasHeaders;

    private int batchSize;

    private Schema finalSchema;

    private static final Logger LOGGER = Logger.getLogger(CSVDataSource.class.getSimpleName());

    public CSVDataSource(String filename, Schema schema, boolean hasHeaders, int batchSize) {
        this.filename = filename;
        this.schema = schema;
        this.hasHeaders = hasHeaders;
        this.batchSize = batchSize;
    }

    public Schema getFinalSchema() {
        Schema finalSchema = null;
        if (this.schema == null) {
            finalSchema = inferSchema();
        } else {
            finalSchema = this.schema;
        }
        this.finalSchema = finalSchema;
        return this.finalSchema;
    }

    private Schema inferSchema() {
        LOGGER.fine("inferSchema()");
        File file = new File(filename);
        if (!file.exists()) {
            throw new RuntimeException(new FileNotFoundException(file.getAbsolutePath()));
        }
        CsvParser parser = buildParser(defaultSettings());
        try (InputStream inputStream = Files.newInputStream(file.toPath());
             Reader reader = new InputStreamReader(inputStream)) {
            parser.beginParsing(reader);
            parser.getDetectedFormat();
            parser.parseNext();
            List<String> headers = Arrays.stream(parser.getContext().parsedHeaders())
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
            Schema schema;
            if (hasHeaders) {
                schema = new Schema(headers.stream()
                        .map(colName -> new Field(colName, ArrowTypes.STRING_TYPE))
                        .collect(Collectors.toList()));
            } else {
                List<Field> candidateFields = new ArrayList<>();
                for (int i = 0; i < headers.size(); i++) {
                    candidateFields.add(new Field("field_" + i, ArrowTypes.STRING_TYPE));
                }
                schema = new Schema(candidateFields);
            }
            parser.stopParsing();
            return schema;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private CsvParser buildParser(CsvParserSettings settings) {
        return new CsvParser(settings);
    }

    private CsvParserSettings defaultSettings() {
        CsvParserSettings settings = new CsvParserSettings();
        settings.setDelimiterDetectionEnabled(true);
        settings.setLineSeparatorDetectionEnabled(true);
        settings.setSkipEmptyLines(true);
        settings.setAutoClosingEnabled(true);
        return settings;
    }


    @Override
    public Schema schema() {
        return getFinalSchema();
    }

    @Override
    public Iterable<RecordBatch> scan(List<String> projection) {
        LOGGER.fine("scan() projection=" + projection.toString());
        File file = new File(filename);
        if (!file.exists()) {
            throw new RuntimeException(new FileNotFoundException(file.getAbsolutePath()));
        }
        Schema readSchema = null;
        if (CollectionUtils.isNotEmpty(projection)) {
            readSchema = getFinalSchema().select(projection);
        } else {
            readSchema = getFinalSchema();
        }
        CsvParserSettings settings = defaultSettings();
        if (CollectionUtils.isNotEmpty(projection)) {
            settings.selectFields(projection.toArray(new String[0]));
        }
        settings.setHeaderExtractionEnabled(hasHeaders);
        if (!hasHeaders) {
            settings.setHeaders(readSchema.getFields().stream()
                    .map(Field::getName)
                    .toArray(String[]::new)
            );
        }
        CsvParser parser = buildParser(settings);
        try {
            // parser will close reader automatically
            parser.beginParsing(new InputStreamReader(new FileInputStream(file)));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        parser.getDetectedFormat();
        return new CSVDataSourceReader(readSchema, parser, batchSize);
    }
}
