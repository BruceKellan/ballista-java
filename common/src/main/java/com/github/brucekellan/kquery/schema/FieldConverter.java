package com.github.brucekellan.kquery.schema;

public class FieldConverter {

    public static Field fromArrowField(org.apache.arrow.vector.types.pojo.Field arrowField) {
        return new Field(arrowField.getName(), arrowField.getFieldType().getType());
    }

}

