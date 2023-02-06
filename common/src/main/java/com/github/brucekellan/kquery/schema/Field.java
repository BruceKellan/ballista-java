package com.github.brucekellan.kquery.schema;

import com.google.common.collect.Lists;
import lombok.Data;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;

@Data
public class Field {

    private String name;

    private ArrowType dataType;

    public Field(String name, ArrowType dataType) {
        this.name = name;
        this.dataType = dataType;
    }

    public org.apache.arrow.vector.types.pojo.Field toArrowField() {
        FieldType fieldType = new FieldType(true, dataType, null);
        return new org.apache.arrow.vector.types.pojo.Field(name, fieldType, Lists.newArrayList());
    }

}
