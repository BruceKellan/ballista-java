package com.github.brucekellan.kquery.sql;

import lombok.Getter;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Getter
public enum Symbol implements TokenType {

    LEFT_PAREN("("),
    RIGHT_PAREN(")"),
    LEFT_BRACE("{"),
    RIGHT_BRACE("}"),
    LEFT_BRACKET("["),
    RIGHT_BRACKET("]"),
    SEMI(";"),
    COMMA(","),
    DOT("."),
    DOUBLE_DOT(".."),
    PLUS("+"),
    SUB("-"),
    STAR("*"),
    SLASH("/"),
    QUESTION("?"),
    EQ("="),
    GT(">"),
    LT("<"),
    BANG("!"),
    TILDE("~"),
    CARET("^"),
    PERCENT("%"),
    COLON(":"),
    DOUBLE_COLON("::"),
    COLON_EQ(":="),
    LT_EQ("<="),
    GT_EQ(">="),
    LT_EQ_GT("<=>"),
    LT_GT("<>"),
    BANG_EQ("!="),
    BANG_GT("!>"),
    BANG_LT("!<"),
    AMP("&"),
    BAR("|"),
    DOUBLE_AMP("&&"),
    DOUBLE_BAR("||"),
    DOUBLE_LT("<<"),
    DOUBLE_GT(">>"),
    AT("@"),
    POUND("#");

    private String text;

    public static Map<String, Symbol> symbols = Arrays.stream(values())
            .collect(Collectors.toMap(Symbol::getText, Function.identity()));

    public static Set<Character> symbolStartSet = Arrays.stream(values())
            .flatMap(symbol -> symbol.getText().chars().mapToObj(i -> (char) i))
            .collect(Collectors.toSet());

    Symbol(String text) {
        this.text = text;
    }

    public static Symbol textOf(String text) {
        return symbols.get(text);
    }

    public static Boolean isSymbol(char ch) {
        return symbolStartSet.contains(ch);
    }

    public static Boolean isSymbolStart(char ch) {
        return isSymbol(ch);
    }
}
