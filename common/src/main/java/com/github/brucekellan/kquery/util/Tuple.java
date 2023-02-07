package com.github.brucekellan.kquery.util;

import lombok.Builder;
import lombok.Getter;

import java.io.Serializable;

@Getter
public class Tuple<F, S> implements Serializable {

    private F first;

    private S second;

    @Builder
    public Tuple(F first, S second) {
        this.first = first;
        this.second = second;
    }

}
