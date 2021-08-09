package com.wenj91.fastgql.core.graphql;

import com.wenj91.fastgql.annotation.Permissions;
import com.wenj91.fastgql.annotation.Roles;
import com.wenj91.fastgql.core.interceptor.DataFetcherInterceptorHandler;
import com.wenj91.fastgql.core.metadata.AuthNaming;
import com.wenj91.fastgql.core.schema.Schema;
import com.wenj91.fastgql.core.sql.Arguments;
import com.wenj91.fastgql.core.sql.MutationExecutor;
import com.wenj91.fastgql.core.sql.QueryExecutor;
import com.wenj91.fastgql.executor.Executor;
import graphql.GraphQL;
import graphql.language.Field;
import graphql.language.OperationDefinition;
import graphql.language.Selection;
import graphql.language.SelectionSet;
import graphql.schema.*;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.stream.Stream;

@Slf4j
public class GraphQLDefinitionBuilder {
  private final Schema schema;
  private final FastGraphQLSchema fastGraphQLSchema;
  private final GraphQLSchema.Builder graphQLSchemaBuilder;
  private final GraphQLCodeRegistry.Builder graphQLCodeRegistryBuilder;
  private final Executor executor;
  private final DataFetcherInterceptorHandler dataFetcherInterceptorHandler;

  private boolean queryEnabled = false;
  private boolean mutationEnabled = false;
  private boolean subscriptionEnabled = false;
  private boolean returningStatementEnabled = false;

  /**
   * 要合并的外部schemas
   */
  private final List<GraphQLSchema> graphQLSchemas = new ArrayList<>();

  /**
   * Class builder, has to be initialized with database schema and SQL connection pool.
   *
   * @param schema input database schema
   */
  public GraphQLDefinitionBuilder(Schema schema, Executor executor, GraphQLSchema... schemas) {
    this.schema = schema;
    this.fastGraphQLSchema = new FastGraphQLSchema(schema);
    this.graphQLSchemaBuilder = GraphQLSchema.newSchema();

    this.graphQLCodeRegistryBuilder = GraphQLCodeRegistry.newCodeRegistry();
    for (GraphQLSchema graphQLSchema : schemas) {
      this.graphQLCodeRegistryBuilder.dataFetchers(graphQLSchema.getCodeRegistry());
      this.graphQLCodeRegistryBuilder.typeResolvers(graphQLSchema.getCodeRegistry());
    }

    this.executor = executor;

    this.dataFetcherInterceptorHandler = new DataFetcherInterceptorHandler(schema.getInterceptors());

    this.graphQLSchemas.addAll(Arrays.asList(schemas));
  }

  private static Stream<Field> createFieldStream(DataFetchingEnvironment env) {
    return env.getDocument().getDefinitions().stream()
        .filter(definition -> definition instanceof OperationDefinition)
        .map(definition -> (OperationDefinition) definition)
        .filter(
            operationDefinition ->
                operationDefinition.getOperation().equals(OperationDefinition.Operation.QUERY))
        .map(operationDefinition -> operationDefinition.getSelectionSet().getSelections())
        .flatMap(List::stream)
        .filter(selection -> selection instanceof Field)
        .map(selection -> (Field) selection);
  }

  private Set<Roles> getRoles(String tableName, String name) {
    return this.schema.getRoles(AuthNaming.getNameRole(tableName, name));
  }

  private Set<Permissions> getPermissions(String tableName, String name) {
    return this.schema.getPermissions(AuthNaming.getNamePermission(tableName, name));
  }

  /**
   * Enables query by defining data fetcher using {@link DataFetcher} and adding it to {@link
   * GraphQLCodeRegistry}.
   *
   * @return this
   */
  public GraphQLDefinitionBuilder enableQuery() {
    if (queryEnabled) {
      return this;
    }

    // graphql query 数据抓取, 返回 list<Map<String, Object>>
    DataFetcher<List<Map<String, Object>>> queryDataFetcher =
        dataFetchingEnvironment -> {
          boolean hasMeta =
              createFieldStream(dataFetchingEnvironment).anyMatch(field -> field.getName().equals("__meta"));
          if (hasMeta) {
            return null;
          } else {
            Field field = dataFetchingEnvironment.getField();
            Object context = dataFetchingEnvironment.getContext();

            // query_by_pk
            String tableName = field.getName();
            boolean handleResult = this.dataFetcherInterceptorHandler.perHandle(
                context,
                this.getRoles(tableName, field.getName()),
                this.getPermissions(tableName, field.getName())
            );

            if (!handleResult) {
              throw new RuntimeException("The result of Interceptor is false");
            }

            List<Map<String, Object>> maps = null;
            Exception e = null;
            try {
              QueryExecutor queryExecutor =
                  new QueryExecutor(fastGraphQLSchema, executor, schema.getDbType());

              // api hook
              maps = queryExecutor.query(field, tableName, null);
            } catch (Exception ex) {
              e = ex;
            } finally {
              this.dataFetcherInterceptorHandler.afterHandle(context, e);
            }

            return maps;
          }

        };

    DataFetcher<Integer> queryCountDataFetcher =
        dataFetchingEnvironment -> {
          boolean hasMeta =
              createFieldStream(dataFetchingEnvironment).anyMatch(field -> field.getName().equals("__meta"));
          if (hasMeta) {
            return null;
          } else {
            Field field = dataFetchingEnvironment.getField();
            Object context = dataFetchingEnvironment.getContext();

            // query_count
            String tableName = field.getName();
            boolean handleResult = this.dataFetcherInterceptorHandler.perHandle(
                context,
                this.getRoles(tableName, field.getName()),
                this.getPermissions(tableName, field.getName())
            );

            if (!handleResult) {
              throw new RuntimeException("The result of Interceptor is false");
            }

            Integer maps = null;
            Exception e = null;
            try {
              QueryExecutor queryExecutor =
                  new QueryExecutor(fastGraphQLSchema, executor, schema.getDbType());

              String querySuffix = "_count";
              if (tableName.endsWith(querySuffix)) {
                tableName = tableName.substring(0, tableName.length() - querySuffix.length());
              }
              // api hook
              maps = queryExecutor.count(field, tableName, null);
            } catch (Exception ex) {
              e = ex;
            } finally {
              this.dataFetcherInterceptorHandler.afterHandle(context, e);
            }

            return maps;
          }
        };

    // graphql query by pk, return Map<String, Object>
    DataFetcher<Map<String, Object>> getDataFetcher =
        dataFetchingEnvironment -> {
          boolean hasMeta =
              createFieldStream(dataFetchingEnvironment).anyMatch(field -> field.getName().equals("__meta"));
          if (!hasMeta) {
            Field field = dataFetchingEnvironment.getField();
            Object context = dataFetchingEnvironment.getContext();

            // query_by_pk
            String tableName = field.getName();
            boolean handleResult = this.dataFetcherInterceptorHandler.perHandle(
                context,
                this.getRoles(tableName, field.getName()),
                this.getPermissions(tableName, field.getName())
            );

            if (!handleResult) {
              throw new RuntimeException("The result of Interceptor is false");
            }

            List<Map<String, Object>> maps;
            Exception e = null;
            try {
              QueryExecutor queryExecutor =
                  new QueryExecutor(fastGraphQLSchema, executor, schema.getDbType());

              String querySuffix = "_by_pk";
              if (tableName.endsWith(querySuffix)) {
                tableName = tableName.substring(0, tableName.length() - querySuffix.length());
              }
              maps = queryExecutor.query(field, tableName, null);
              if (maps.size() > 0) {
                return maps.get(0);
              }
            } catch (Exception exception) {
              e = exception;
            } finally {
              this.dataFetcherInterceptorHandler.afterHandle(context, e);
            }
          }
          return null;
        };
    DataFetcher<Map<String, Object>> getAggregateDataFetcher =
        dataFetchingEnvironment -> {
          boolean hasMeta =
              createFieldStream(dataFetchingEnvironment).anyMatch(field -> field.getName().equals("__meta"));
          if (!hasMeta) {
            Field field = dataFetchingEnvironment.getField();
            Object context = dataFetchingEnvironment.getContext();

            // query_by_pk
            String tableName = field.getName();
            String querySuffix = "_aggregate";
            if (tableName.endsWith(querySuffix)) {
              tableName = tableName.substring(0, tableName.length() - querySuffix.length());
            }

            boolean handleResult = this.dataFetcherInterceptorHandler.perHandle(
                context,
                this.getRoles(tableName, field.getName()),
                this.getPermissions(tableName, field.getName())
            );

            if (!handleResult) {
              throw new RuntimeException("The result of Interceptor is false");
            }

            Exception e = null;
            Map<String, Object> res = new HashMap<>();
            try {
              SelectionSet selectionSet = field.getSelectionSet();
              if (selectionSet != null) {
                List<Selection> selections = selectionSet.getSelections();

                for (Selection selection : selections) {
                  Field f = (Field) selection;
                  if (f.getName().equals("nodes")) {
                    QueryExecutor queryExecutor =
                        new QueryExecutor(fastGraphQLSchema, executor, schema.getDbType());

                    Arguments arguments = new Arguments(
                        field.getArguments(),
                        tableName,
                        null,
                        fastGraphQLSchema
                    );


                    List<Map<String, Object>> query = queryExecutor.query(f, tableName, arguments.getCondition());
                    res.put("nodes", query);
                  }

                  if (f.getName().equals("aggregate")) {
                    QueryExecutor queryExecutor =
                        new QueryExecutor(fastGraphQLSchema, executor, schema.getDbType());

                    Arguments arguments = new Arguments(
                        field.getArguments(),
                        tableName,
                        null,
                        fastGraphQLSchema
                    );

                    List<Map<String, Object>> aggregateMaps = queryExecutor.query(f, tableName, arguments.getCondition());
                    if (!aggregateMaps.isEmpty()) {
                      Map<String, Object> aggregate = new HashMap<>(aggregateMaps.get(0));
                      res.put("aggregate", aggregate);
                    }
                  }
                }
              }
            } catch (Exception exception) {
              e = exception;
            } finally {
              this.dataFetcherInterceptorHandler.afterHandle(context, e);
            }

            return res;
          }
          return null;
        };

    DataFetcher<Map<String, Object>> metaDataFetcher =
        dataFetchingEnvironment -> {
          //                Map<String, Object> userParams = createUserParamsQuery(env);
          //                QueryFunctions queryFunctions = createExecutionFunctions(userParams);
          //
          //                Map<String, String> mockQueries =
          //                    createFieldStream(env)
          //                        .filter(field -> !field.getName().equals("__meta"))
          //                        .map(
          //                            field ->
          //                                Map.entry(
          //                                    field.getName(),
          //                                    String.join("; ", queryFunctions.createQueriesToExecute(field))
          //                                        .strip()))
          //                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
          //
          //                promise.complete(Map.of("sql", mockQueries, "user", userParams.toString()));
          return null;
        };

    // graphQLCodeRegistryBuilder 注册data fetcher
    // 流程：服务启动时生成graphql图信息，注册data fetcher
    //       graphql client发起请求 -> graphQLCodeRegistry -> data fetcher
    schema
        .getTableNames()
        .forEach(
            tableName -> {
              // Query {table(where:...):[table]}
              graphQLCodeRegistryBuilder.dataFetcher(
                  FieldCoordinates.coordinates("Query", tableName), queryDataFetcher);

              graphQLCodeRegistryBuilder.dataFetcher(
                  FieldCoordinates.coordinates("Query", tableName + "_count"), queryCountDataFetcher);

              // Query {table_by_pk(id:Int!):table}
              if (fastGraphQLSchema.isRegisterDataFetcherForQueryByPk(tableName)) {
                graphQLCodeRegistryBuilder.dataFetcher(
                    FieldCoordinates.coordinates("Query", tableName + "_by_pk"), getDataFetcher);
              }

              graphQLCodeRegistryBuilder.dataFetcher(
                  FieldCoordinates.coordinates("Query", tableName + "_aggregate"), getAggregateDataFetcher);
            });
    graphQLCodeRegistryBuilder.dataFetcher(
        FieldCoordinates.coordinates("Query", "__meta"), metaDataFetcher);
    queryEnabled = true;
    return this;
  }

  /**
   * Enables mutation by defining data fetcher using {@link DataFetcher} and adding it to
   * {@link GraphQLCodeRegistry}.
   *
   * @return this
   */
  public GraphQLDefinitionBuilder enableMutation() {
    if (mutationEnabled) {
      return this;
    }

    DataFetcher<Map<String, Object>> mutationDataFetcher =
        dataFetchingEnvironment -> {
          Field field = dataFetchingEnvironment.getField();
          Object context = dataFetchingEnvironment.getContext();

          String insertSuffix = "_insert";

          if (!field.getName().endsWith(insertSuffix)) {
            throw new RuntimeException("Field name does not end with _insert");
          }

          String tableName = field.getName().substring(0, field.getName().length() - insertSuffix.length());

          boolean handleResult = this.dataFetcherInterceptorHandler.perHandle(
              context,
              this.getRoles(tableName, field.getName()),
              this.getPermissions(tableName, field.getName())
          );

          if (!handleResult) {
            throw new RuntimeException("The result of Interceptor is false");
          }

          Exception e = null;
          Map<String, Object> result = null;
          try {
            MutationExecutor mutationExecutor = new MutationExecutor(this.fastGraphQLSchema, this.executor);
            result = mutationExecutor.mutation(field,
                dataFetchingEnvironment.getArgument("objects"), this.schema.getDbType());
          } catch (Exception exception) {
            e = exception;
          } finally {
            this.dataFetcherInterceptorHandler.afterHandle(context, e);
          }

          return result;
        };


    DataFetcher<Map<String, Object>> deleteByPkMutationDataFetcher =
        dataFetchingEnvironment -> {
          Field field = dataFetchingEnvironment.getField();
          Object context = dataFetchingEnvironment.getContext();

          String deleteFlag = "_delete";

          if (!field.getName().contains(deleteFlag)) {
            throw new RuntimeException("Field name does not end with _insert");
          }

          String tableName = field.getName().substring(0, field.getName().indexOf(deleteFlag));

          boolean handleResult = this.dataFetcherInterceptorHandler.perHandle(
              context,
              this.getRoles(tableName, field.getName()),
              this.getPermissions(tableName, field.getName())
          );

          if (!handleResult) {
            throw new RuntimeException("The result of Interceptor is false");
          }

          Exception e = null;
          Map<String, Object> result = null;
          try {
            MutationExecutor mutationExecutor = new MutationExecutor(this.fastGraphQLSchema, this.executor);
            result = mutationExecutor.mutation(field, null, this.schema.getDbType());
          } catch (Exception exception) {
            e = exception;
          } finally {
            this.dataFetcherInterceptorHandler.afterHandle(context, e);
          }

          return result;
        };

    // VertxDataFetcher<Map<String, Object>> mutationDataFetcher =
    //  new VertxDataFetcher<>(
    //    (env, promise) ->
    //            sqlConnectionPool
    //                .rxBegin()
    //                .flatMap(
    //                    transaction ->
    //                        getResponseMutation(env, transaction)
    //                            .flatMap(
    //                                result ->
    //                                    transaction.rxCommit().andThen(Single.just(result))))
    //                .subscribe(promise::complete, promise::fail));

    DataFetcher<Map<String, Object>> updateMutationDataFetcher =
        dataFetchingEnvironment -> {
          Field field = dataFetchingEnvironment.getField();
          Object context = dataFetchingEnvironment.getContext();

          String updateFlag = "_update";

          if (!field.getName().contains(updateFlag)) {
            throw new RuntimeException("Field name does not end with _insert");
          }

          String tableName = field.getName().substring(0, field.getName().indexOf(updateFlag));

          boolean handleResult = this.dataFetcherInterceptorHandler.perHandle(
              context,
              this.getRoles(tableName, field.getName()),
              this.getPermissions(tableName, field.getName())
          );

          if (!handleResult) {
            throw new RuntimeException("The result of Interceptor is false");
          }

          Exception e = null;
          Map<String, Object> result = null;
          try {
            MutationExecutor mutationExecutor = new MutationExecutor(this.fastGraphQLSchema, this.executor);
            result = mutationExecutor.mutation(field, dataFetchingEnvironment.getArgument("object"), this.schema.getDbType());
          } catch (Exception exception) {
            e = exception;
          } finally {
            this.dataFetcherInterceptorHandler.afterHandle(context, e);
          }

          return result;
        };

    schema
        .getTableNames()
        .forEach(
            tableName -> {
              graphQLCodeRegistryBuilder.dataFetcher(
                  FieldCoordinates.coordinates(
                      "Mutation", String.format("%s_insert", tableName)),
                  mutationDataFetcher);

              if (fastGraphQLSchema.isRegisterDataFetcherForQueryByPk(tableName)) {
                graphQLCodeRegistryBuilder.dataFetcher(
                    FieldCoordinates.coordinates(
                        "Mutation", String.format("%s_delete_by_pk", tableName)),
                    deleteByPkMutationDataFetcher);
              }

              graphQLCodeRegistryBuilder.dataFetcher(
                  FieldCoordinates.coordinates(
                      "Mutation", String.format("%s_delete", tableName)),
                  deleteByPkMutationDataFetcher);

              if (fastGraphQLSchema.isRegisterDataFetcherForQueryByPk(tableName)) {
                graphQLCodeRegistryBuilder.dataFetcher(
                    FieldCoordinates.coordinates(
                        "Mutation", String.format("%s_update_by_pk", tableName)),
                    updateMutationDataFetcher);
              }

              graphQLCodeRegistryBuilder.dataFetcher(
                  FieldCoordinates.coordinates(
                      "Mutation", String.format("%s_update", tableName)),
                  updateMutationDataFetcher);

            });

    mutationEnabled = true;
    return this;
  }


//    /**
//     * Enables subscription by defining subscription data fetcher and adding it to {@link
//     * GraphQLCodeRegistry}.
//     *
//     * @param vertx          vertx instance
//     * @param debeziumConfig debezium config
//     * @return this
//     */
//    public Builder enableSubscription(Vertx vertx, DebeziumConfig debeziumConfig) {
//
//      if (subscriptionEnabled || !debeziumConfig.isActive()) {
//        log.debug("Subscription already enabled or debezium is not configured");
//        return this;
//      }
//
//      if (debeziumConfig.isEmbedded()) {
//        try {
//          debeziumEngineSingleton.startNewEngine();
//        } catch (IOException e) {
//          log.error("subscription not enabled: debezium engine could not start");
//          return this;
//        }
//      }
//
//      DataFetcher<Flowable<List<Map<String, Object>>>> subscriptionDataFetcher =
//          env -> {
//            ExecutionDefinition<List<Map<String, Object>>> executionDefinition =
//                createQueryExecutionDefinition(env, createUserParamsSubscription(env));
//            return eventFlowableFactory
//                .create(executionDefinition.getQueriedTables())
//                .flatMapSingle(record -> sqlConnectionPool.rxBegin())
//                .flatMapMaybe(transaction -> executeTransaction(transaction, executionDefinition))
//                .defaultIfEmpty(List.of());
//          };
//
//      databaseSchema
//          .getTableNames()
//          .forEach(
//              tableName ->
//                  graphQLCodeRegistryBuilder.dataFetcher(
//                      FieldCoordinates.coordinates("Subscription", tableName),
//                      subscriptionDataFetcher));
//      subscriptionEnabled = true;
//      return this;
//    }

  /**
   * Build {@link GraphQL} by applying internally constructed {@link FastGraphQLSchema} to
   * query / subscription builders.
   *
   * @return constructed GraphQL object
   */
  public GraphQL build() {
    if (!(queryEnabled || subscriptionEnabled)) {
      throw new RuntimeException("query or subscription has to be enabled");
    }

    GraphQLObjectType.Builder queryBuilder = GraphQLObjectType.newObject().name("Query");
    GraphQLObjectType.Builder subscriptionBuilder = GraphQLObjectType.newObject().name("Subscription");
    GraphQLObjectType.Builder mutationBuilder = GraphQLObjectType.newObject().name("Mutation");

    for (GraphQLSchema graphQLSchema : this.graphQLSchemas) {
      if (graphQLSchema.getQueryType() != null) {
        queryBuilder = GraphQLObjectType.newObject(graphQLSchema.getQueryType()); // GraphQLObjectType.newObject().name("Query");
      }

      if (graphQLSchema.getSubscriptionType() != null) {
        subscriptionBuilder = GraphQLObjectType.newObject(graphQLSchema.getSubscriptionType()); // GraphQLObjectType.newObject().name("Subscription");
      }

      if (graphQLSchema.getMutationType() != null) {
        mutationBuilder = GraphQLObjectType.newObject(graphQLSchema.getMutationType()); // GraphQLObjectType.newObject().name("Mutation");
      }
    }

    List<GraphQLObjectType.Builder> builders = new ArrayList<>();
    if (queryEnabled) {
      builders.add(queryBuilder);
    }

    if (subscriptionEnabled) {
      builders.add(subscriptionBuilder);
    }

    this.fastGraphQLSchema.applyToGraphQLObjectTypes(builders);

    if (queryEnabled) {
      this.fastGraphQLSchema.applyToQueryObjectType(queryBuilder);
      graphQLSchemaBuilder.query(queryBuilder);
    }

    if (subscriptionEnabled) {
      graphQLSchemaBuilder.subscription(subscriptionBuilder);
    }

    if (mutationEnabled) {
      this.fastGraphQLSchema.applyMutation(mutationBuilder, returningStatementEnabled);
      graphQLSchemaBuilder.mutation(mutationBuilder);
    }

    GraphQLSchema graphQLSchema =
        graphQLSchemaBuilder.codeRegistry(graphQLCodeRegistryBuilder.build()).build();
    return GraphQL.newGraphQL(graphQLSchema).build();
  }
}
