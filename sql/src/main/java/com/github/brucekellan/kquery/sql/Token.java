package com.github.brucekellan.kquery.sql;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class Token {

    private String text;

    private TokenType type;

    private Integer endOffset;

    public Token(String text, TokenType type, Integer endOffset) {
        this.text = text;
        this.type = type;
        this.endOffset = endOffset;
    }

    @Override
    public String toString() {
        String type = null;
        if (this.type instanceof Literal) {
            type = "Literal";
        } else if (this.type instanceof Symbol) {
            type = "Symbol";
        } else if (this.type instanceof Keyword) {
            type = "Keyword";
        } else {
            type = "";
        }
        return String.format("Token(\"%s\", %s.%s, %s)", this.text, type, this.type, this.endOffset);
    }
}
