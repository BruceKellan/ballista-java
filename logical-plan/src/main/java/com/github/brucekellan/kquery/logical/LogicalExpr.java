package com.github.brucekellan.kquery.logical;

import com.github.brucekellan.kquery.logical.expr.*;
import com.github.brucekellan.kquery.schema.Field;

public interface LogicalExpr {

    Field toField(LogicalPlan input);

    default LogicalExpr eq(LogicalExpr rhs) {
        return new Eq(this, rhs);
    }

    default LogicalExpr neq(LogicalExpr rhs) {
        return new Neq(this, rhs);
    }

    default LogicalExpr gt(LogicalExpr rhs) {
        return new Gt(this, rhs);
    }

    default LogicalExpr gteq(LogicalExpr rhs) {
        return new GtEq(this, rhs);
    }

    default LogicalExpr lt(LogicalExpr rhs) {
        return new Lt(this, rhs);
    }

    default LogicalExpr lteq(LogicalExpr rhs) {
        return new LtEq(this, rhs);
    }

    default LogicalExpr add(LogicalExpr rhs) {
        return new Add(this, rhs);
    }

    default LogicalExpr subtract(LogicalExpr rhs) {
        return new Subtract(this, rhs);
    }

    default LogicalExpr mult(LogicalExpr rhs) {
        return new Multiply(this, rhs);
    }

    default LogicalExpr div(LogicalExpr rhs) {
        return new Divide(this, rhs);
    }

    default LogicalExpr mod(LogicalExpr rhs) {
        return new Modulus(this, rhs);
    }

    default LogicalExpr alias(String alias) {
        return new Alias(this, alias);
    }
}
