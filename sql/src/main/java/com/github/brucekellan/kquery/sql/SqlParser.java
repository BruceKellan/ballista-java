package com.github.brucekellan.kquery.sql;

import com.github.brucekellan.kquery.sql.expression.SqlExpr;

public class SqlParser implements ParttParser{

    private TokenStream tokens;

    public SqlParser(TokenStream tokens) {
        this.tokens = tokens;
    }

    @Override
    public int nextPrecedence() {
        return 0;
    }

    @Override
    public SqlExpr parsePrefix() {
        return null;
    }

    @Override
    public SqlExpr parseInfix(SqlExpr left, int precedence) {
        return null;
    }
}
