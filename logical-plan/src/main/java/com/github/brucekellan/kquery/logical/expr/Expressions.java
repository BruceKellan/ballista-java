package com.github.brucekellan.kquery.logical.expr;

import com.github.brucekellan.kquery.logical.LogicalExpr;
import org.apache.arrow.vector.types.pojo.ArrowType;

public class Expressions {

    public static Column col(String name) {
        return new Column(name);
    }

    public static LiteralString lit(String value) {
        return new LiteralString(value);
    }

    public static LiteralLong lit(Long n) {
        return new LiteralLong(n);
    }

    public static LiteralFloat lit(Float n) {
        return new LiteralFloat(n);
    }

    public static LiteralDouble lit(Double n) {
        return new LiteralDouble(n);
    }

    public static CastExpr cast(LogicalExpr expr, ArrowType dataType) {
        return new CastExpr(expr, dataType);
    }

}
