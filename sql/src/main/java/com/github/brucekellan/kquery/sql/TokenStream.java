package com.github.brucekellan.kquery.sql;

import java.util.List;
import java.util.Objects;
import java.util.logging.Logger;

public class TokenStream {

    private static final Logger LOGGER = Logger.getLogger(TokenStream.class.getSimpleName());

    private List<Token> tokens;

    private int i = 0;

    public TokenStream(List<Token> tokens) {
        this.tokens = tokens;
        this.i = 0;
    }

    public Token peek() {
        if (this.i < tokens.size()) {
            return tokens.get(i);
        } else {
            return null;
        }
    }

    public Token next() {
        if (this.i < tokens.size()) {
            return tokens.get(i++);
        } else {
            return null;
        }
    }

    public Boolean consumeKeyword(String s) {
        Token peek = peek();
        LOGGER.fine(String.format("consumeKeyword('%s') next token is %s", s, peek));
        if (peek != null && peek.getType() instanceof Keyword && Objects.equals(peek.getText(), s)) {
            i++;
            LOGGER.fine("consumeKeyword() returning true");
            return true;
        } else {
            LOGGER.fine("consumeKeyword() returning false");
            return false;
        }
    }

    public Boolean consumeTokenType(TokenType t) {
        Token peek = peek();
        if (peek != null && peek.getType() == t) {
            i++;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "";
    }
}
