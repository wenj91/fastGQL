package com.wenj91.fastgql.core.sql;

import com.wenj91.fastgql.core.graphql.FastGraphQLSchema;
import lombok.Getter;
import lombok.ToString;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 表查询对象结构
 */
@Getter
@ToString
public class Table {
  private final String name;
  /**
   * 表名
   */
  private final String tableName;
  /**
   * 表别名
   */
  private final String tableAlias;
  /**
   * 排序
   */
  private final String orderBy;
  /**
   * 去重（注意：MySQL的去重逻辑与PostgreSQL的不太一样）
   */
  private final String distinctOn;
  /**
   * 分页limit
   */
  private final String limit;
  /**
   * 分页offset
   */
  private final String offset;
  /**
   * 查询参数
   */
  private final PreparedQuery conditionFromArguments;
  private final List<Object> params;
  private final Map<String, Object> jwtParams;
  private final FastGraphQLSchema schema;
  private final String pathInQuery;
  private Condition extraCondition;

  public Table(
      String name,
      String tableName,
      String tableAlias,
      Arguments arguments,
      Condition extraCondition,
      Map<String, Object> jwtParams,
      FastGraphQLSchema schema,
      String pathInQuery) {
    this.name = name;
    this.tableName = tableName;
    this.tableAlias = tableAlias;
    this.jwtParams = jwtParams;
    this.schema = schema;
    this.conditionFromArguments =
        arguments.getCondition() == null
            ? PreparedQuery.create()
            : ConditionUtils.conditionToSQL(arguments.getCondition(), tableAlias, jwtParams, schema);
    this.params =
        Stream.of(conditionFromArguments)
            .flatMap(preparedQuery -> preparedQuery.getParams().stream())
            .collect(Collectors.toUnmodifiableList());
    this.orderBy =
        arguments.getOrderByList() == null
            ? ""
            : OrderByUtils.orderByToSQL(arguments.getOrderByList());

    this.distinctOn = arguments.getDistinctOn() == null
        ? ""
        : DistinctOnUtils.distinctOnToSQL(arguments.getDistinctOn());

    this.limit = arguments.getLimit() == null ? "" : arguments.getLimit().toString();
    this.offset = arguments.getOffset() == null ? "" : arguments.getOffset().toString();
    this.extraCondition = extraCondition;
    this.pathInQuery = pathInQuery;
  }

  public void setExtraCondition(Condition extraCondition) {
    this.extraCondition = extraCondition;
  }

  public List<Object> createParams() {
    return Stream.concat(
        params.stream(),
        extraCondition == null
            ? Stream.of()
            : ConditionUtils.conditionToSQL(extraCondition, tableAlias, jwtParams, schema)
            .getParams()
            .stream())
        .collect(Collectors.toUnmodifiableList());
  }

  public PreparedQuery getWhere() {
    return Stream.of(
        conditionFromArguments,
        extraCondition == null
            ? PreparedQuery.create()
            : ConditionUtils.conditionToSQL(extraCondition, tableAlias, jwtParams, schema))
        .collect(PreparedQuery.collectorWithAnd());
  }

  String sqlString() {
    return String.format("%s %s", tableName, tableAlias);
  }
}
