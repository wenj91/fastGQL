package com.wenj91.fastgql.core.sql;


import com.wenj91.fastgql.common.enums.AggregateType;
import com.wenj91.fastgql.common.enums.DBType;
import com.wenj91.fastgql.core.util.StringUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Query {

  private int resultAliasCount = 0;
  private final Table table;
  private final List<SelectColumn> selectColumns;
  private final List<LeftJoin> leftJoins;
  private final List<Table> queriedTables = new ArrayList<>();

  public Query(Table table) {
    this.table = table;
    this.selectColumns = new ArrayList<>();
    this.leftJoins = new ArrayList<>();
    this.queriedTables.add(this.table);
  }

  public Table getTable() {
    return table;
  }

  private String getNextResultAlias() {
    resultAliasCount++;
    return String.format("v%d", resultAliasCount);
  }

  public SelectColumn addSelectColumn(Table table, Column column) {
    SelectColumn selectColumn = new SelectColumn(table, column, getNextResultAlias());
    selectColumns.add(selectColumn);
    return selectColumn;
  }

  public SelectColumn aggregate(Table table, Column column, AggregateType type) {
    SelectColumn selectColumn = new SelectColumn(table, column, getNextResultAlias(), type.getFunc());
    selectColumns.add(selectColumn);
    return selectColumn;
  }

  public SelectColumn addLeftJoin(
      Table table, Column column, Table foreignTable, String foreignColumnName) {
    SelectColumn selectColumn = addSelectColumn(table, column);
    leftJoins.add(new LeftJoin(table, column.getColumnName(), foreignTable, foreignColumnName));
    queriedTables.add(foreignTable);
    return selectColumn;
  }

  public List<Object> buildParams() {
    return queriedTables.stream()
        .flatMap(table -> table.createParams().stream())
        .collect(Collectors.toList());
  }

  public String buildQuery(DBType dbType) {
    StringBuilder select = new StringBuilder();

    String distinctOnSqlString = table.getDistinctOn();
    if (!StringUtil.isBlank(distinctOnSqlString)) {
      select.append("DISTINCT ");
      if (dbType == DBType.postgresql) {
        select.append(" ON(");
      }

      select.append(distinctOnSqlString);

      if (dbType == DBType.postgresql) {
        select.append(") ");
      }
    }

    String selectColumnsStr = selectColumns.stream()
        .map(SelectColumn::sqlString)
        .collect(Collectors.joining(", "));
    if (!StringUtil.isBlank(selectColumnsStr)
        && !StringUtil.isBlank(select.toString())
        && dbType != DBType.postgresql) {
      select.append(", ");
    }

    select.append(selectColumnsStr);

    if (select.length() <= 0) {
      select.append("*");
    }

    PreparedQuery preparedQuery =
        PreparedQuery.create()
            .merge(
                String.format(
                    "SELECT %s FROM %s %s",
                    select,
                    table.sqlString(),
                    leftJoins.stream().map(LeftJoin::sqlString).collect(Collectors.joining(" "))));

    PreparedQuery wherePreparedQuery =
        queriedTables.stream().map(Table::getWhere).collect(PreparedQuery.collectorWithAnd());

    String orderBySqlString = table.getOrderBy();
    String limitSqlString = table.getLimit();
    String offsetSqlString = table.getOffset();

    if (!wherePreparedQuery.isEmpty()) {
      preparedQuery.merge(" WHERE ").merge(wherePreparedQuery);
    }
    if (!orderBySqlString.isEmpty()) {
      preparedQuery.merge(" ORDER BY ").merge(orderBySqlString);
    }
    if (!limitSqlString.isEmpty()) {
      preparedQuery.merge(" LIMIT ").merge(limitSqlString);
    }
    if (!offsetSqlString.isEmpty()) {
      preparedQuery.merge(" OFFSET ").merge(offsetSqlString);
    }

    return preparedQuery.buildQuery(dbType);
  }

}
