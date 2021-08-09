/*
 * Copyright fastGQL Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package com.wenj91.fastgql.core.graphql;

import com.wenj91.fastgql.core.schema.Schema;
import com.wenj91.fastgql.executor.Executor;
import graphql.GraphQL;
import graphql.schema.GraphQLSchema;
import lombok.extern.slf4j.Slf4j;

/**
 * Class to build {@link GraphQL} from {@link Schema} (used for defining GraphQL schema) and
 * <p>
 * 构建graphql api，以及data fetchers（重点注释文件）
 *
 * @author Kamil Bobrowski
 */
@Slf4j
public class GraphQLDefinition {

  private static volatile GraphQLDefinitionBuilder builder;

  public synchronized static GraphQLDefinitionBuilder create(
      Schema schema,
      Executor executor,
      GraphQLSchema... graphQLSchema
  ) {
    if (builder == null) {
      builder = new GraphQLDefinitionBuilder(schema, executor, graphQLSchema);
    }

    return builder;
  }

}
