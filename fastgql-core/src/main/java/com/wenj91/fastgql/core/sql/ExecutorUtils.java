package com.wenj91.fastgql.core.sql;

import com.wenj91.fastgql.common.enums.ReferenceType;
import com.wenj91.fastgql.core.graphql.FastGraphQLSchema;
import com.wenj91.fastgql.core.graphql.GraphQLField;
import graphql.language.Field;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.wenj91.fastgql.common.enums.ReferenceType.REFERENCED;

public class ExecutorUtils {
  private static Map<String, TableAlias> createPathInQueryToTableAlias(FastGraphQLSchema schema,
      String currentPathInQuery, String currentAlias, String tableName, Field field) {

    tableName = tableName.replaceAll("_delete", "");
    tableName = tableName.replaceAll("_update", "");
    tableName = tableName.replaceAll("_by_pk", "");
    if (tableName.contains("_on_")) {
      tableName = tableName.substring(0, tableName.indexOf("_on_"));
    }

    if (tableName.endsWith("_aggregate")) {
      tableName = tableName.substring(0, tableName.indexOf("_aggregate"));
    }

    Map<String, TableAlias> pathInQueryToAlias = new HashMap<>();
    String newPathInQuery =
        currentPathInQuery != null
            ? String.format("%s/%s", currentPathInQuery, field.getName())
            : field.getName();
    pathInQueryToAlias.put(newPathInQuery, new TableAlias(tableName, currentAlias));

    AtomicInteger count = new AtomicInteger();

    String finalTableName = tableName;
    return (field.getSelectionSet() == null ?
        new ArrayList<>() :
        field.getSelectionSet().getSelections()).stream()
        .filter(selection -> selection instanceof Field)
        .map(selection -> (Field) selection)
        .filter(subField -> !subField.getName().equals("__sql"))
        .map(
            subField -> {
              GraphQLField graphQLField =
                  schema.fieldAt(finalTableName, subField.getName());
              return
                  graphQLField == null ?
                      createPathInQueryToTableAlias(
                          schema,
                          newPathInQuery,
                          String.format("%s_%d", currentAlias, count.getAndIncrement()),
                          finalTableName,
                          subField)
                      :
                      graphQLField.getReferenceType() == ReferenceType.REFERENCING
                          || graphQLField.getReferenceType() == REFERENCED
                          ? createPathInQueryToTableAlias(
                          schema,
                          newPathInQuery,
                          String.format("%s_%d", currentAlias, count.getAndIncrement()),
                          graphQLField.getForeignName().getTableName(),
                          subField)
                          : new HashMap<String, TableAlias>();
            })
        .reduce(
            pathInQueryToAlias,
            (accumulated, current) -> {
              accumulated.putAll(current);
              return accumulated;
            })
        .entrySet()
        .stream()
        .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  public static Map<String, TableAlias> createPathInQueryToTableAlias(FastGraphQLSchema schema, Field field) {
    return createPathInQueryToTableAlias(schema, null, "t", field.getName(), field);
  }

  public static Map<String, TableAlias> createPathInQueryToTableAlias(FastGraphQLSchema schema, Field field, String tableName) {
    return createPathInQueryToTableAlias(schema, null, "t", tableName, field);
  }

  public static Map<String, String> createPathInQueryToAlias(
      Map<String, TableAlias> pathInQueryToTableAlias) {
    return pathInQueryToTableAlias.entrySet().stream()
        .collect(
            Collectors.toUnmodifiableMap(
                Map.Entry::getKey, entry -> entry.getValue().getTableAlias()));
  }

  public static Set<TableAlias> createTableAliases(Map<String, TableAlias> pathInQueryToTableAlias) {
    return new HashSet<>(pathInQueryToTableAlias.values());
  }

}
