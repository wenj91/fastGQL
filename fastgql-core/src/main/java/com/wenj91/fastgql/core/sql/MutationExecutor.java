package com.wenj91.fastgql.core.sql;

import com.wenj91.fastgql.common.enums.DBType;
import com.wenj91.fastgql.core.graphql.FastGraphQLSchema;
import com.wenj91.fastgql.core.graphql.GraphQLField;
import com.wenj91.fastgql.core.schema.TableInfo;
import com.wenj91.fastgql.core.util.StringUtil;
import com.wenj91.fastgql.executor.Executor;
import com.wenj91.fastgql.executor.Result;
import graphql.language.Argument;
import graphql.language.Field;
import graphql.schema.GraphQLArgument;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;


@Slf4j
public class MutationExecutor {
  private final FastGraphQLSchema fastGraphQLSchema;
  private final Executor executor;

  public MutationExecutor(FastGraphQLSchema fastGraphQLSchema, Executor executor) {
    this.fastGraphQLSchema = fastGraphQLSchema;
    this.executor = executor;
  }

  static class QueryParams {
    private final String sql;
    private final List<Object> params;

    QueryParams(String sql, List<Object> params) {
      this.sql = sql;
      this.params = params;
    }
  }

  static class Pair<L, R> {
    private final L left;
    private final R right;

    Pair(L value, R alias) {
      this.left = value;
      this.right = alias;
    }

    static <L, R> Pair<L, R> of(L left, R right) {
      return new Pair<>(left, right);
    }
  }


  private QueryParams buildInsertMutationQueryFromRow(
      TableInfo tableInfo, Map<String, Object> row, DBType dbType) {

    PlaceholderCounter placeholderCounter = new PlaceholderCounter(dbType);

    List<Map.Entry<String, Object>> columnNameToValueArguments =
        row.entrySet().stream()
            .map(entry -> {
              GraphQLField graphQLField =
                  fastGraphQLSchema.fieldAt(tableInfo.getName(), entry.getKey());

              if (null == graphQLField) {
                throw new RuntimeException("mutation column not found! -- [" + entry.getKey() + "]");
              }

              return Map.entry(graphQLField.getQualifiedName().getKeyName(), entry.getValue());
            })
            .collect(Collectors.toList());

    List<String> columnsSqlListArguments =
        columnNameToValueArguments.stream()
            .map(Map.Entry::getKey)
            .collect(Collectors.toUnmodifiableList());


    Map<String, Pair<Object, String>> columnNameToValueAlias =
        columnNameToValueArguments.stream()
            .collect(
                (Supplier<HashMap<String, Object>>) HashMap::new,
                (accumulator, next) -> accumulator.put(next.getKey(), next.getValue()),
                Map::putAll)
            .entrySet()
            .stream()
            .collect(
                LinkedHashMap::new,
                (accumulator, next) ->
                    accumulator.put(
                        next.getKey(), Pair.of(next.getValue(), placeholderCounter.next())),
                Map::putAll);

    Map<String, Object> columnNameToValue =
        columnNameToValueAlias.entrySet().stream()
            .collect(
                Collectors.toUnmodifiableMap(Map.Entry::getKey, entry -> entry.getValue().left));


    String columnsSql = String.join(", ", columnNameToValueAlias.keySet());

    String valuesSql =
        columnNameToValueAlias.values().stream()
            .map(valueAlias -> valueAlias.right)
            .collect(Collectors.joining(", "));

    List<Object> params =
        columnNameToValueAlias.values().stream()
            .map(valueAlias -> valueAlias.left)
            .collect(Collectors.toUnmodifiableList());

    return new QueryParams(
        String.format(
            "INSERT INTO %s (%s) VALUES (%s)",
            tableInfo.getTableName(),
            String.join(", ", columnsSql),
            String.join(", ", valuesSql)),
        params);
  }


  private QueryParams buildUpdateMutationQueryFromRow(
      TableInfo tableInfo,
      Map<String, Object> row,
      PlaceholderCounter placeholderCounter
  ) {

    GraphQLArgument pk = fastGraphQLSchema.getPk(tableInfo.getName());

    List<Map.Entry<String, Object>> columnNameToValueArguments =
        row.entrySet().stream()
            .map(entry -> {
              GraphQLField graphQLField =
                  fastGraphQLSchema.fieldAt(tableInfo.getName(), entry.getKey());

              if (null != pk &&
                  pk.getName().equals(
                      graphQLField.getQualifiedName().getKeyName())) {
                return null;
              }

              if (null == graphQLField) {
                throw new RuntimeException("mutation column not found! -- [" + entry.getKey() + "]");
              }

              return Map.entry(graphQLField.getQualifiedName().getKeyName(), entry.getValue());
            })
            .filter(Objects::nonNull)
            .collect(Collectors.toList());


    List<String> columnsSqlListArguments =
        columnNameToValueArguments.stream()
            .map(Map.Entry::getKey)
            .collect(Collectors.toUnmodifiableList());


    Map<String, Pair<Object, String>> columnNameToValueAlias =
        columnNameToValueArguments.stream()
            .collect(
                (Supplier<HashMap<String, Object>>) HashMap::new,
                (accumulator, next) -> accumulator.put(next.getKey(), next.getValue()),
                Map::putAll)
            .entrySet()
            .stream()
            .collect(
                LinkedHashMap::new,
                (accumulator, next) ->
                    accumulator.put(
                        next.getKey(), Pair.of(next.getValue(), placeholderCounter.next())),
                Map::putAll);

    Map<String, Object> columnNameToValue =
        columnNameToValueAlias.entrySet().stream()
            .collect(
                Collectors.toUnmodifiableMap(Map.Entry::getKey, entry -> entry.getValue().left));


    List<String> columnsSql = new ArrayList<>();

    for (String key : columnNameToValueAlias.keySet()) {
      columnsSql.add(String.format("%s=%s", key, columnNameToValueAlias.get(key).right));
    }

    List<Object> params =
        columnNameToValueAlias.values().stream()
            .map(valueAlias -> valueAlias.left)
            .collect(Collectors.toUnmodifiableList());

    return new QueryParams(
        String.format(
            "UPDATE %s SET %s",
            tableInfo.getTableName(),
            String.join(",", columnsSql)),
        params);
  }

  private Map<String, Object> insert(String tableName, Field field, Object rowsObject, DBType dbType) {

    TableInfo tableInfo = fastGraphQLSchema.tableInfo(tableName);

    if (null == tableInfo) {
      throw new RuntimeException("table info not found");
    }

    Optional<Argument> rowObjectOptional =
        field.getArguments().stream()
            .filter(argument -> argument.getName().equals("objects"))
            .findFirst();

    if (rowObjectOptional.isEmpty()) {
      throw new RuntimeException("No 'objects' argument in mutation");
    }

    List<Map<String, Object>> rows = (List<Map<String, Object>>) rowsObject;

    List<QueryParams> queries =
        rows.stream()
            .map(row -> buildInsertMutationQueryFromRow(tableInfo, row, dbType))
            .collect(Collectors.toList());


    List<Result> results = queries.stream().map(item -> {
      String uuid = UUID.randomUUID().toString();

      log.info("{} - mutation sql:{} - param:{}", uuid, item.sql, item.params);

      Result result = executor.exec(item.sql, item.params.toArray());

      log.info("{} - mutation res:{}", uuid, result);

      return result;
    }).collect(Collectors.toList());

    Integer affected = results.stream()
        .map(Result::getRowsAffected)
        .reduce(0, Integer::sum);

    String ids = results.stream()
        .map(Result::getLastInsertId)
        .map(String::valueOf).collect(Collectors.joining());

    return Map.of("affected_rows", affected,
        "last_insert_ids", ids);
  }

  private Map<String, Object> update(String tableName, Field field, Object rowsObject, DBType dbType) {

    TableInfo tableInfo = fastGraphQLSchema.tableInfo(tableName);

    if (null == tableInfo) {
      throw new RuntimeException("table info not found");
    }

    Optional<Argument> rowObjectOptional =
        field.getArguments().stream()
            .filter(argument -> argument.getName().equals("object"))
            .findFirst();

    if (rowObjectOptional.isEmpty()) {
      throw new RuntimeException("No 'object' argument in mutation");
    }

    PlaceholderCounter placeholderCounter = new PlaceholderCounter(dbType);

    Map<String, Object> row = (Map<String, Object>) rowsObject;

    QueryParams queryParams = buildUpdateMutationQueryFromRow(tableInfo, row, placeholderCounter);

    List<Argument> condWhere =
        field.getArguments().stream()
            .filter(argument -> argument.getName().equals("where"))
            .collect(Collectors.toList());

    String tableAlias = "";
    Arguments arguments =
        new Arguments(condWhere, tableName, tableAlias, fastGraphQLSchema);

    PreparedQuery query =
        arguments.getCondition() == null
            ? PreparedQuery.create()
            : ConditionUtils.conditionToSQL(arguments.getCondition(), tableAlias, new HashMap<>(), fastGraphQLSchema);
    List<Object> params =
        Stream.of(query)
            .flatMap(preparedQuery -> preparedQuery.getParams().stream())
            .collect(Collectors.toList());

    String cond = query.buildQuery(placeholderCounter);

    StringBuilder sb = new StringBuilder();
    if (!StringUtil.isBlank(cond)) {
      sb.append(" WHERE ");
      sb.append(cond);
    }

    GraphQLArgument pk = fastGraphQLSchema.getPk(tableInfo.getName());
    if (pk != null && field.getName().endsWith("_by_pk")) {
      boolean pkExist = false;
      for (Map.Entry<String, Object> item : row.entrySet()) {
        if (item.getKey().equals(pk.getName())) {
          pkExist = true;
          GraphQLField graphQLField =
              fastGraphQLSchema.fieldAt(tableInfo.getName(), item.getKey());
          if (null == graphQLField) {
            throw new RuntimeException("Table graphql pk field is not exist! [" + pk.getName() + "]");
          }

          if (sb.length() <= 0) {
            sb.append(" WHERE ");
          } else {
            sb.append(" AND ");
          }

          sb.append(String.format("%s=%s",
              graphQLField.getQualifiedName().getKeyName(),
              placeholderCounter.next()));
          query.getParams().add(item.getValue());
        }
      }

      if (!pkExist) {
        throw new RuntimeException("update_by_pk primary key must be not null");
      }
    }

    List<Object> mergeParams = new ArrayList<>();
    mergeParams.addAll(queryParams.params);
    mergeParams.addAll(query.getParams());

    StringBuilder mergeSql = new StringBuilder(queryParams.sql);
    mergeSql.append(sb);

    String uuid = UUID.randomUUID().toString();

    log.info("{} - mutation sql:{} - param:{}", uuid, mergeSql, mergeParams);
    Result result = executor.exec(mergeSql.toString(), mergeParams.toArray());
    log.info("{} - mutation res:{}", uuid, result);

    Integer affected = result.getRowsAffected();

    return Map.of("affected_rows", affected,
        "last_insert_ids", "");
  }

  private Map<String, Object> delete(String tableName, Field field, Object rowsObject, DBType dbType) {
    TableInfo tableInfo = fastGraphQLSchema.tableInfo(tableName);

    if (null == tableInfo) {
      throw new RuntimeException("table info not found");
    }

    final Set<TableAlias> tableAliasesFromQuery = new HashSet<>();

    String tableAlias = "";
    Arguments arguments =
        new Arguments(field.getArguments(), tableName, tableAlias, fastGraphQLSchema);

    PreparedQuery query =
        arguments.getCondition() == null
            ? PreparedQuery.create()
            : ConditionUtils.conditionToSQL(arguments.getCondition(), tableAlias, new HashMap<>(), fastGraphQLSchema);
    List<Object> params =
        Stream.of(query)
            .flatMap(preparedQuery -> preparedQuery.getParams().stream())
            .collect(Collectors.toUnmodifiableList());


    String cond = query.buildQuery(dbType);

    String uuid = UUID.randomUUID().toString();
    String sql = String.format(
        "DELETE FROM %s WHERE %s",
        tableInfo.getTableName(),
        cond);

    log.info("{} - mutation sql:{} - param:{}", uuid, sql, params);
    Result result = executor.exec(sql, params.toArray());
    log.info("{} - mutation res:{}", uuid, result);

    Integer affected = result.getRowsAffected();

    return Map.of("affected_rows", affected);
  }

  public Map<String, Object> mutation(
      Field field, Object rowsObject, DBType dbType) {

    String fieldName = field.getName();


    String insertSuffix = "_insert";
    if (field.getName().endsWith(insertSuffix)) {
      String tableName = fieldName.substring(0, field.getName().length() - insertSuffix.length());

      return this.insert(tableName, field, rowsObject, dbType);
    }

    String deleteFlag = "_delete";
    if (field.getName().contains(deleteFlag)) {
      String tableName = fieldName.substring(0, field.getName().indexOf(deleteFlag));

      return this.delete(tableName, field, rowsObject, dbType);
    }

    String updateFlag = "_update";
    if (field.getName().contains(updateFlag)) {
      String tableName = fieldName.substring(0, field.getName().indexOf(updateFlag));
      return this.update(tableName, field, rowsObject, dbType);
    }


    return null;
  }
}