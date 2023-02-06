package com.github.brucekellan.kquery.schema;

import java.util.stream.Collectors;

public class SchemaConverter {

    public static Schema fromArrowSchema(org.apache.arrow.vector.types.pojo.Schema arrowSchema) {
        return new Schema(arrowSchema.getFields().stream()
                .map(FieldConverter::fromArrowField)
                .collect(Collectors.toList()));
    }

}
