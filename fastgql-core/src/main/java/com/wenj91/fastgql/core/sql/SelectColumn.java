package com.wenj91.fastgql.core.sql;

import lombok.Data;

@Data
public class SelectColumn {
  private final String tableAlias;
  private final Column column;
  private final String resultAlias;
  private final String func;

  SelectColumn(Table table, Column column, String resultAlias, String func) {
    this.tableAlias = table.getTableAlias();
    this.column = column;
    this.resultAlias = resultAlias;
    this.func = func;
  }

  SelectColumn(Table table, Column column, String resultAlias) {
    this.tableAlias = table.getTableAlias();
    this.column = column;
    this.resultAlias = resultAlias;
    this.func = null;
  }

  SelectColumn(String tableAlias, Column column) {
    this.tableAlias = tableAlias;
    this.column = column;
    this.resultAlias = null;
    this.func = null;
  }

  String sqlString() {
    String col = String.format("%s.%s", tableAlias, column.getColumnName());

    if (func != null) {
      col = String.format("%s(%s)", func, col);
    }

    if (resultAlias == null) {
      return col;
    } else {
      return String.format("%s AS %s", col, resultAlias);
    }
  }
}
