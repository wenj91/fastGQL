package com.wenj91.fastgql.core.sql;

import com.wenj91.fastgql.core.graphql.FastGraphQLSchema;
import com.wenj91.fastgql.core.graphql.GraphQLField;
import graphql.language.Argument;
import graphql.language.ArrayValue;
import graphql.language.EnumValue;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DistinctOnUtils {

  public static String distinctOnToSQL(List<DistinctOn> distinctOnList) {
    return distinctOnList.stream().map(DistinctOn::sqlString).collect(Collectors.joining(", "));
  }

  private static Stream<DistinctOn> createDistinctOnStream(
      Column column,
      String tableName,
      String tableAlias) {
    return Stream.of(
        new DistinctOn(
            new SelectColumn(tableAlias, column))
    );
  }

  static List<DistinctOn> createDistinctOn(
      Argument argument,
      String tableName,
      String tableAlias,
      FastGraphQLSchema fastGraphQLSchema) {
    if (argument.getValue() instanceof ArrayValue) {
      Stream<DistinctOn> distinctOnStream = ((ArrayValue) argument.getValue())
          .getValues().stream()
          .flatMap(
              value -> {
                EnumValue val = (EnumValue) value;
                GraphQLField graphQLField = fastGraphQLSchema.fieldAt(tableName, val.getName());
                if (null == graphQLField) {
                  throw new RuntimeException("DistinctOn field is not found! [" + val.getName() + "]");
                }

                Column column = Column.builder()
                    .columnName(graphQLField.getQualifiedName().getKeyName())
                    .columnNameAlias(graphQLField.getName().getKeyName())
                    .build();

                return createDistinctOnStream(
                    column,
                    tableName,
                    tableAlias);
              });

      return distinctOnStream.collect(Collectors.toList());
    }


    EnumValue val = (EnumValue) argument.getValue();
    GraphQLField graphQLField = fastGraphQLSchema.fieldAt(tableName, val.getName());
    if (null == graphQLField) {
      throw new RuntimeException("DistinctOn field is not found! [" + val.getName() + "]");
    }

    Column column = Column.builder()
        .columnName(graphQLField.getQualifiedName().getKeyName())
        .columnNameAlias(graphQLField.getName().getKeyName())
        .build();

    Stream<DistinctOn> orderByStream = createDistinctOnStream(
        column,
        tableName,
        tableAlias
    );
    return orderByStream.collect(Collectors.toList());
  }
}
