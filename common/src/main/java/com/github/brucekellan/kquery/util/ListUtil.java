package com.github.brucekellan.kquery.util;

import java.util.ArrayList;
import java.util.List;

public class ListUtil {

    public static <T> List<T> concat(List<T> left, List<T> right) {
        List<T> all = new ArrayList<>(left.size() + right.size());
        all.addAll(left);
        all.addAll(right);
        return all;
    }

}
