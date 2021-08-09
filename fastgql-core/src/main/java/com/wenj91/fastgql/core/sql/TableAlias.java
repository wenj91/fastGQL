package com.wenj91.fastgql.core.sql;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import java.util.Objects;

@Getter
@ToString
@AllArgsConstructor
public class TableAlias {
  private final String tableName;
  private final String tableAlias;

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TableAlias that = (TableAlias) o;
    return Objects.equals(tableName, that.tableName)
        && Objects.equals(tableAlias, that.tableAlias);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableName, tableAlias);
  }
}
