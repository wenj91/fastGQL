package com.wenj91.fastgql.core.sql;


public class DistinctOn {
  private final SelectColumn selectColumn;

  DistinctOn(SelectColumn selectColumn) {
    this.selectColumn = selectColumn;
  }

  public String sqlString() {
    return selectColumn.sqlString();
  }


  @Override
  public String toString() {
    return String.format(
        "DistinctOn<selectColumn=%s>", selectColumn);
  }
}
