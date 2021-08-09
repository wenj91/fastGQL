package com.wenj91.fastgql.core.graphql;

import graphql.schema.GraphQLEnumType;
import graphql.schema.GraphQLInputType;
import graphql.schema.GraphQLList;

import java.util.Collection;
import java.util.List;

public class DistinctOn {

  public static GraphQLEnumType distinctOn(
      String tableName,
      String distinctOnName,
      Collection<String> distinctOns
  ) {
    GraphQLEnumType.Builder enumBuilder = GraphQLEnumType.newEnum()
        .name(distinctOnName)
        .description(distinctOnName);

    for (String column : distinctOns) {
      enumBuilder.value(column, column, tableName + " distinct_on column: " + column)
          .build();
    }

    return enumBuilder.build();
  }
}
