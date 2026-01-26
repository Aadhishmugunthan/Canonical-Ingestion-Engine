package com.poc.CanonicalIngestionEngine.sql;

import java.util.Set;
import java.util.stream.Collectors;

public class SqlBuilder {
    public static String build(String tpl, Set<String> cols){
        String c = String.join(",", cols);
        String v = cols.stream().map(x->":"+x).collect(Collectors.joining(","));
        return tpl.replace("%COLUMNS%",c).replace("%VALUES%",v);
    }
}

