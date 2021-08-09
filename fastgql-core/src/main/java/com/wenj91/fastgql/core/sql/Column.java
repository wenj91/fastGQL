package com.wenj91.fastgql.core.sql;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Column {
    private final String columnName;
    private final String columnNameAlias;

    public Column(String columnName, String columnNameAlias) {
        this.columnName = columnName;
        this.columnNameAlias = columnNameAlias;
    }
}
