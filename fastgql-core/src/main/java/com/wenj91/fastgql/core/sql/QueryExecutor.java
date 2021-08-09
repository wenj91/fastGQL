package com.wenj91.fastgql.core.sql;

import com.wenj91.fastgql.common.enums.AggregateType;
import com.wenj91.fastgql.common.enums.DBType;
import com.wenj91.fastgql.common.enums.ReferenceType;
import com.wenj91.fastgql.common.enums.RelationalOperator;
import com.wenj91.fastgql.core.graphql.FastGraphQLSchema;
import com.wenj91.fastgql.core.graphql.GraphQLField;
import com.wenj91.fastgql.core.schema.TableInfo;
import com.wenj91.fastgql.executor.Executor;
import graphql.language.Field;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.wenj91.fastgql.common.enums.AggregateType.*;
import static com.wenj91.fastgql.common.enums.ReferenceType.REFERENCED;

@Slf4j
public class QueryExecutor {
  private final FastGraphQLSchema fastGraphQLSchema;

  private final Executor executor;

  private final DBType dbType;

  public QueryExecutor(FastGraphQLSchema fastGraphQLSchema, Executor executor, DBType dbType) {
    this.fastGraphQLSchema = fastGraphQLSchema;
    this.executor = executor;
    this.dbType = dbType;
  }

  private List<SelectColumn> aggregateColumns(
      Table table,
      Field field,
      Query query,
      Map<String, String> pathInQueryToAlias,
      String pathInQuery,
      String func
  ) {
    return field.getSelectionSet().getSelections().stream()
        .filter(selection -> selection instanceof Field)
        .map(selection -> (Field) selection)
        .map(
            subField -> {
              GraphQLField graphQLField =
                  fastGraphQLSchema.fieldAt(table.getTableName(), subField.getName());
              String columnName = graphQLField.getQualifiedName().getKeyName();
              String beanFieldName = graphQLField.getName().getKeyName();

              Column column = Column.builder()
                  .columnName(columnName)
                  .columnNameAlias(beanFieldName)
                  .build();

              AggregateType aggregateType = getByName(func);
              return query.aggregate(table, column, aggregateType);
            }).collect(Collectors.toList());
  }

  private SelectColumns selectColumns(
      Table table,
      Field field,
      Query query,
      Map<String, String> pathInQueryToAlias,
      String pathInQuery
  ) {

    SelectColumns selectColumns = new SelectColumns();

    GraphQLField graphQLField =
        fastGraphQLSchema.fieldAt(table.getName(), field.getName());
    String columnName = graphQLField.getQualifiedName().getKeyName();
    String beanFieldName = graphQLField.getName().getKeyName();
    Column column = Column.builder()
        .columnName(columnName)
        .columnNameAlias(beanFieldName)
        .build();

    switch (graphQLField.getReferenceType()) {
      case NONE:
        SelectColumn selectColumn = query.addSelectColumn(table, column);
        selectColumns.select(selectColumn);
        break;
      case REFERENCING:
        String refEntity = graphQLField.getForeignName().getTableName();
        String refField = graphQLField.getForeignName().getKeyName();

        String newPathInQuery = String.format("%s/%s", pathInQuery, field.getName());

        TableInfo info = fastGraphQLSchema.tableInfo(refEntity);
        Objects.requireNonNull(info, "table info must be not null");

        Table foreignTable =
            new Table(
                info.getName(),
                info.getTableName(),
                pathInQueryToAlias.get(newPathInQuery),
                new Arguments(),
                null,
                new HashMap<>(),
                fastGraphQLSchema,
                newPathInQuery);
        SelectColumn selectColumnReferencing =
            query.addLeftJoin(table, column, foreignTable, refField);

        SelectColumns columns = columns(foreignTable, field, query, pathInQueryToAlias, newPathInQuery);
        selectColumns.referencing(field.getName(), columns);
        break;
      case REFERENCED:
        SelectColumn referencedColumn = query.addSelectColumn(table, column);
        String foreignColumnNameForReferenced = graphQLField.getForeignName().getKeyName();
        String foreignTableNameForReferenced = graphQLField.getForeignName().getTableName();

        GraphQLField foreignGraphQLField = fastGraphQLSchema.fieldAt(foreignTableNameForReferenced, foreignColumnNameForReferenced);
        Objects.requireNonNull(foreignGraphQLField, "foreign graphql field must be not null");

        String finalColumnName = foreignGraphQLField.getQualifiedName().getKeyName();

        selectColumns.referenced(field.getName(),
            new ReferencedColumn(field, finalColumnName, referencedColumn));
        break;
      default:
        throw new RuntimeException(
            "Unknown reference type: " + graphQLField.getReferenceType());
    }

    return selectColumns;
  }

  private SelectColumns columns(
      Table table,
      Field field,
      Query query,
      Map<String, String> pathInQueryToAlias,
      String pathInQuery
  ) {
    SelectColumns selectColumns = new SelectColumns();
    (field.getSelectionSet() == null ?
        new ArrayList<>() :
        field.getSelectionSet().getSelections())
        .stream()
        .filter(selection -> selection instanceof Field)
        .map(selection -> (Field) selection)
        .forEach(
            subField -> {
              String subFieldName = subField.getName();

              AggregateType aggregateType = AggregateType.getByName(subFieldName);
              if (null != aggregateType) {
                List<SelectColumn> aggregateColumns =
                    aggregateColumns(table, subField, query, pathInQueryToAlias, pathInQuery, subFieldName);

                selectColumns.aggregate(aggregateType, aggregateColumns);
              } else {
                SelectColumns columns = selectColumns(table, subField, query, pathInQueryToAlias, pathInQuery);
                selectColumns.selects(columns.getSelect());
                selectColumns.referencings(columns.getReferencing());
                selectColumns.referenceds(columns.getReferenced());
              }
            });

    return selectColumns;
  }

  public List<Map<String, Object>> query(Field field, String tableName, Condition condition) {
    String uuid = UUID.randomUUID().toString();

    final Set<TableAlias> tableAliasesFromQuery = new HashSet<>();

    Map<String, TableAlias> pathInQueryToTableAlias =
        ExecutorUtils.createPathInQueryToTableAlias(fastGraphQLSchema, field, tableName);
    Map<String, String> pathInQueryToAlias = ExecutorUtils.createPathInQueryToAlias(pathInQueryToTableAlias);
    Set<TableAlias> tableAliases = ExecutorUtils.createTableAliases(pathInQueryToTableAlias);

    log.info("id:{} - table:{}", uuid, tableName);

    String pathInQuery = field.getName();

    String tableAlias = pathInQueryToAlias.get(pathInQuery);

    Arguments arguments =
        new Arguments(field.getArguments(), tableName, tableAlias, fastGraphQLSchema);

    TableInfo info = fastGraphQLSchema.tableInfo(tableName);
    Objects.requireNonNull(info);

    Table table = new Table(
        info.getName(),
        info.getTableName(),
        tableAlias,
        arguments,
        condition,
        new HashMap<>(),
        fastGraphQLSchema,
        pathInQuery);

    Query query = new Query(table);

    SelectColumns selectColumns = columns(table, field, query, pathInQueryToAlias, pathInQuery);

    String queryString = query.buildQuery(dbType);
    List<Object> objects = query.buildParams();

    log.info("id:{} - query:{} params:{}", uuid, queryString, objects);

    List<Map<String, Object>> queryResult = executor.query(queryString, objects.toArray());
    List<Map<String, Object>> res = this.rowSetProc(queryResult, selectColumns);

    log.info("id:{} - res:{}", uuid, res);
    return res;
  }

  public Integer count(Field field, String tableName, Condition condition) {
    String uuid = UUID.randomUUID().toString();

    final Set<TableAlias> tableAliasesFromQuery = new HashSet<>();

    Map<String, TableAlias> pathInQueryToTableAlias =
        ExecutorUtils.createPathInQueryToTableAlias(fastGraphQLSchema, field, tableName);
    Map<String, String> pathInQueryToAlias = ExecutorUtils.createPathInQueryToAlias(pathInQueryToTableAlias);
    Set<TableAlias> tableAliases = ExecutorUtils.createTableAliases(pathInQueryToTableAlias);

    log.info("id:{} - table:{}", uuid, tableName);

    String pathInQuery = field.getName();

    String tableAlias = pathInQueryToAlias.get(pathInQuery);

    Arguments arguments =
        new Arguments(field.getArguments(), tableName, tableAlias, fastGraphQLSchema);

    TableInfo info = fastGraphQLSchema.tableInfo(tableName);
    Objects.requireNonNull(info);

    Table table = new Table(
        info.getName(),
        info.getTableName(),
        tableAlias,
        arguments,
        condition,
        new HashMap<>(),
        fastGraphQLSchema,
        pathInQuery);

    Query query = new Query(table);

    SelectColumns selectColumns = columns(table, field, query, pathInQueryToAlias, pathInQuery);

    String queryString = query.buildQuery(dbType);
    List<Object> objects = query.buildParams();

    log.info("id:{} - query:{} params:{}", uuid, queryString, objects);

    String sql = "SELECT count(1) from (" + queryString + ")ct";

    Integer queryResult = executor.count(sql, objects.toArray());

    log.info("id:{} - res:{}", uuid, queryResult);
    return queryResult;
  }

  public List<Map<String, Object>> rowSetProc(List<Map<String, Object>> queryResult, SelectColumns selectColumns) {
    List<Map<String, Object>> res = new ArrayList<>();

    for (Map<String, Object> item : queryResult) {
      Map<String, Object> normalColumns = this.selectColumns(selectColumns, item);
      if (null == normalColumns) {
        continue;
      }

      Map<AggregateType, Map<String, Object>> aggregate = this.aggregate(selectColumns, item);
      aggregate.forEach((type, aggregateRes) -> {
        if (!aggregateRes.isEmpty()) {
          normalColumns.put(type.getName(), aggregateRes);
        }
      });

      Map<String, Object> referencingColumns = this.referencingColumns(selectColumns, item);
      if (referencingColumns != null) {
        normalColumns.putAll(referencingColumns);
      }

      Map<String, ReferencedColumn> referenced = selectColumns.getReferenced();
      if (!referenced.isEmpty()) {
        Map<String, Object> referencedColumns = this.referencedColumns(selectColumns, item);
        normalColumns.putAll(referencedColumns);
      }

      res.add(normalColumns);
    }

    return res;
  }

  public Map<String, Object> selectColumns(SelectColumns selectColumns, Map<String, Object> queryResult) {
    Map<String, Object> res = new HashMap<>();
    for (SelectColumn selectColumn : selectColumns.getSelect()) {
      String resultAlias = selectColumn.getResultAlias();
      Object object = queryResult.get(resultAlias);
      if (null != object) {
        res.put(selectColumn.getColumn().getColumnName(), object);
        res.put(selectColumn.getColumn().getColumnNameAlias(), object);
      }
    }

    return res;
  }


  public Map<AggregateType, Map<String, Object>> aggregate(
      SelectColumns selectColumns,
      Map<String, Object> queryResult
  ) {

    Map<AggregateType, Map<String, Object>> res = new HashMap<>();
    for (AggregateType type : AggregateType.values()) {

      Map<String, Object> aggregateMap = new HashMap<>();
      for (SelectColumn selectColumn : selectColumns.getAggregate(type)) {
        String resultAlias = selectColumn.getResultAlias();
        Object object = queryResult.get(resultAlias);
        if (null != object) {
          aggregateMap.put(selectColumn.getColumn().getColumnName(), object);
          aggregateMap.put(selectColumn.getColumn().getColumnNameAlias(), object);
        }
      }

      res.put(type, aggregateMap);
    }

    return res;
  }

  public Map<String, Object> referencingColumns(SelectColumns selectColumns, Map<String, Object> queryResult) {
    Map<String, Object> res = new HashMap<>();
    Set<Map.Entry<String, SelectColumns>> entries = selectColumns.getReferencing().entrySet();
    for (Map.Entry<String, SelectColumns> entry : entries) {
      Map<String, Object> item = this.selectColumns(entry.getValue(), queryResult);
      res.put(entry.getKey(), item);

      SelectColumns curSelectColumns = entry.getValue();
      Map<String, SelectColumns> referencing = curSelectColumns.getReferencing();
      if (!referencing.isEmpty()) {
        Map<String, Object> referencingColumns = this.referencedColumns(curSelectColumns, queryResult);
        if (referencingColumns != null) {
          res.putAll(referencingColumns);
        }
      }

      Map<String, ReferencedColumn> referenced = curSelectColumns.getReferenced();
      if (!referenced.isEmpty()) {
        Map<String, Object> referencedColumns = this.referencedColumns(curSelectColumns, queryResult);
        item.putAll(referencedColumns);
      }
    }
    return res;
  }

  public Map<String, Object> referencedColumns(SelectColumns selectColumns, Map<String, Object> queryResult) {
    Map<String, Object> res = new HashMap<>();
    Map<String, ReferencedColumn> referenced = selectColumns.getReferenced();
    Set<Map.Entry<String, ReferencedColumn>> entriesTemp = referenced.entrySet();
    for (Map.Entry<String, ReferencedColumn> entryTemp : entriesTemp) {
      ReferencedColumn referencedColumn = entryTemp.getValue();
      SelectColumn selectColumn = referencedColumn.getSelectColumn();

      Object object = queryResult.get(selectColumn.getResultAlias());

      Condition extCond = new Condition(referencedColumn.getForeignColumnNameForReferenced(),
          RelationalOperator._eq,
          params -> object);

      // query_by_pk
      String referencedTableName = referencedColumn.getField().getName();

      String queryReferencedSuffix = "_on_";
      if (referencedTableName.contains(queryReferencedSuffix)) {
        referencedTableName = referencedTableName.substring(0, referencedTableName.indexOf(queryReferencedSuffix));
      }

      List<Map<String, Object>> maps = query(referencedColumn.getField(), referencedTableName, extCond);
      res.put(entryTemp.getKey(), maps);
    }
    return res;
  }
}
