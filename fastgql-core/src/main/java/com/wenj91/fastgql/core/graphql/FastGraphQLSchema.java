/*
 * Copyright fastGQL Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package com.wenj91.fastgql.core.graphql;

import com.wenj91.fastgql.common.enums.AggregateType;
import com.wenj91.fastgql.common.enums.ReferenceType;
import com.wenj91.fastgql.common.types.QualifiedName;
import com.wenj91.fastgql.core.schema.Schema;
import com.wenj91.fastgql.core.schema.TableInfo;
import graphql.GraphQL;
import graphql.schema.*;
import lombok.Data;
import lombok.ToString;

import java.util.*;
import java.util.stream.Collectors;

import static com.wenj91.fastgql.common.enums.AggregateType.*;
import static graphql.Scalars.*;

/**
 * <p>
 * Data structure defining GraphQL schema, including standard fields as well as one-to-one and
 * one-to-many relationships between tables which are inferred from foreign keys. This class is
 * constructed from {@link Schema} and is used it two ways: as a helper in building {@link
 * GraphQL} or as a helper in parsing {@link DataFetchingEnvironment}
 * </p>
 * 通过数据库表结构信息构建graphql的图信息
 *
 * @author Kamil Bobrowski
 */
@ToString
public class FastGraphQLSchema {
  /**
   * 表对应字段数据
   */
  private final Map<String, Map<String, GraphQLField>> graph;
  /**
   * 数据库表结构信息
   */
  private final Schema schema;
  /**
   * graphql分页查询limit
   */
  private final GraphQLArgument limit;
  /**
   * graphql分页查询offset
   */
  private final GraphQLArgument offset;
  /**
   * graphql排序查询
   */
  private final Map<String, GraphQLArgument> orderByMap;

  private final Map<String, GraphQLArgument> distinctOnMap;
  /**
   * graphql条件查询
   */
  private final Map<String, GraphQLArgument> whereMap;

  private final Map<String, GraphQLArgument> pkMap;

  public GraphQLField fieldAt(String table, String field) {
    return graph.get(table).get(field);
  }

  public TableInfo tableInfo(String name) {
    return schema.getTables().get(name);
  }

  /**
   * Constructs object from {@link Schema}.
   *
   * @param schema input schema
   */
  public FastGraphQLSchema(Schema schema) {
    this.graph = createGraph(schema);
    this.schema = schema;
    this.limit = createArgument("limit", GraphQLInt);
    this.offset = createArgument("offset", GraphQLInt);
    this.orderByMap = createOrderByMap(schema);
    this.distinctOnMap = createDistinctOnMap(schema);
    this.whereMap = createWhereMap(schema);
    this.pkMap = createPKMap(schema);
  }

  public boolean isRegisterDataFetcherForQueryByPk(String tableName) {
    return this.pkMap.containsKey(tableName);
  }

  public GraphQLArgument getPk(String tableName) {
    return this.pkMap.get(tableName);
  }

  /**
   * Applies this schema to given {@link GraphQLObjectType} Mutation object builder.
   * <p>
   * graphql变更操作信息构建
   *
   * @param builder builder to which this schema will be applied
   */
  public void applyMutation(GraphQLObjectType.Builder builder, boolean returningStatementEnabled) {
    Objects.requireNonNull(builder);
    schema
        .getGraph()
        .forEach(
            (tableName, keyNameToKeyDefinition) -> {
              GraphQLObjectType.Builder rowObjectBuilder =
                  GraphQLObjectType.newObject().name(String.format("%s_row", tableName));

              // input参数构建
              GraphQLInputObjectType.Builder rowInputBuilder =
                  GraphQLInputObjectType.newInputObject()
                      .name(String.format("%s_input", tableName));

              keyNameToKeyDefinition.forEach(
                  (keyName, keyDefinition) -> {
                    rowObjectBuilder.field(
                        GraphQLFieldDefinition.newFieldDefinition()
                            .name(keyName)
                            .type(GraphQLField.keyTypeToGraphQLType.get(keyDefinition.getKeyType()))
                            .build());
                    rowInputBuilder.field(
                        GraphQLInputObjectField.newInputObjectField()
                            .name(keyName)
                            .type(GraphQLField.keyTypeToGraphQLType.get(keyDefinition.getKeyType()))
                            .build());
                  });

              GraphQLObjectType rowObject = rowObjectBuilder.build();
              GraphQLInputObjectType rowInput = rowInputBuilder.build();

              // output参数构建
              GraphQLObjectType.Builder outputObjectBuilder =
                  GraphQLObjectType.newObject()
                      .name(String.format("%s_output", tableName))
                      .field(
                          GraphQLFieldDefinition.newFieldDefinition()
                              .name("affected_rows")
                              .type(GraphQLInt)
                              .build());
              if (returningStatementEnabled) {
                outputObjectBuilder.field(
                    GraphQLFieldDefinition.newFieldDefinition()
                        .name("returning")
                        .type(GraphQLList.list(rowObject))
                        .build());
              }

              // 构建insert mutation
              builder.field(
                  GraphQLFieldDefinition.newFieldDefinition()
                      .name(String.format("%s_insert", tableName))
                      .type(outputObjectBuilder)
                      .argument(
                          GraphQLArgument.newArgument()
                              .name("objects")
                              .type(GraphQLList.list(rowInput))
                              .build())
                      .build());

              if (pkMap.containsKey(tableName)) {
                GraphQLObjectType.Builder updateByIdOutputObjectBuilder =
                    GraphQLObjectType.newObject()
                        .name(String.format("%s_update_by_pk_output", tableName))
                        .field(
                            GraphQLFieldDefinition.newFieldDefinition()
                                .name("affected_rows")
                                .type(GraphQLInt)
                                .build());

                // 构建update mutation
                builder.field(
                    GraphQLFieldDefinition.newFieldDefinition()
                        .name(String.format("%s_update_by_pk", tableName))
                        .type(updateByIdOutputObjectBuilder)
                        .argument(
                            GraphQLArgument.newArgument()
                                .name("object")
                                .type(rowInput)
                                .build())
                        .build());
              }

              GraphQLObjectType.Builder updateOutputObjectBuilder =
                  GraphQLObjectType.newObject()
                      .name(String.format("%s_update_output", tableName))
                      .field(
                          GraphQLFieldDefinition.newFieldDefinition()
                              .name("affected_rows")
                              .type(GraphQLInt)
                              .build());

              // 构建update mutation
              builder.field(
                  GraphQLFieldDefinition.newFieldDefinition()
                      .name(String.format("%s_update", tableName))
                      .type(updateOutputObjectBuilder)
                      .argument(
                          GraphQLArgument.newArgument()
                              .name("object")
                              .type(rowInput)
                              .build())
                      .argument(whereMap.get(tableName))
                      .build());


              if (pkMap.containsKey(tableName)) {
                GraphQLObjectType.Builder deleteByPkOutputObjectBuilder =
                    GraphQLObjectType.newObject()
                        .name(String.format("%s_delete_by_pk_output", tableName))
                        .field(
                            GraphQLFieldDefinition.newFieldDefinition()
                                .name("affected_rows")
                                .type(GraphQLInt)
                                .build());

                // 构建delete mutation
                builder.field(
                    GraphQLFieldDefinition.newFieldDefinition()
                        .name(String.format("%s_delete_by_pk", tableName))
                        .type(deleteByPkOutputObjectBuilder)
                        .argument(pkMap.get(tableName))
                        .build());
              }

              GraphQLObjectType.Builder deleteOutputObjectBuilder =
                  GraphQLObjectType.newObject()
                      .name(String.format("%s_delete_output", tableName))
                      .field(
                          GraphQLFieldDefinition.newFieldDefinition()
                              .name("affected_rows")
                              .type(GraphQLInt)
                              .build());

              // 构建delete mutation
              builder.field(
                  GraphQLFieldDefinition.newFieldDefinition()
                      .name(String.format("%s_delete", tableName))
                      .type(deleteOutputObjectBuilder)
                      .argument(whereMap.get(tableName))
                      .build());

            });

  }

  /**
   * graphql查询图对象类型构建
   * ps:
   * __meta: __sql
   *
   * @param builder
   */
  public void applyToQueryObjectType(GraphQLObjectType.Builder builder) {
    Objects.requireNonNull(builder);

    GraphQLObjectType.Builder sqlInternalObjectTypeBuilder =
        GraphQLObjectType.newObject().name("__sql_internal");

    graph.forEach(
        (tableName, fieldNameToGraphQLField) -> {
          sqlInternalObjectTypeBuilder.field(
              GraphQLFieldDefinition.newFieldDefinition()
                  .name(tableName)
                  .type(GraphQLString)
                  .build());
        });

    GraphQLObjectType sqlObjectType =
        GraphQLObjectType.newObject()
            .name("__sql")
            .field(
                GraphQLFieldDefinition.newFieldDefinition()
                    .name("sql")
                    .type(sqlInternalObjectTypeBuilder)
                    .build())
            .field(
                GraphQLFieldDefinition.newFieldDefinition()
                    .name("user")
                    .type(GraphQLString)
                    .build())
            .build();

    builder.field(
        GraphQLFieldDefinition.newFieldDefinition().name("__meta").type(sqlObjectType).build());
  }

  private Map<AggregateType, GraphQLObjectType.Builder> aggregateBuilder(String tableName) {
    Map<AggregateType, GraphQLObjectType.Builder> res = new HashMap<>();
    for (AggregateType type : AggregateType.values()) {
      res.put(type, GraphQLObjectType.newObject().name(String.format(type.getFormat(), tableName)));
    }

    return res;
  }

  private GraphQLObjectType.Builder aggregateFieldsBuilder(String tableName,
                                                           Map<AggregateType, GraphQLObjectType.Builder> builderMap) {
    GraphQLObjectType.Builder aggregateBuilder = GraphQLObjectType.newObject()
        .name(tableName + "_aggregate_fields");

    builderMap.forEach((type, builder) -> {
      aggregateBuilder.field(GraphQLFieldDefinition.newFieldDefinition()
          .name(type.getName())
          .type(builder.build())
      );
    });

    return aggregateBuilder;
  }


  /**
   * Applies this schema to given {@link GraphQLObjectType} builders (e.g. Query or Subscription
   * object builders).
   * <p>
   * graphql对象列表查询构建<br/>
   * ps:<br/>
   * <code>
   * addresses(<br/>
   * &nbsp;&nbsp;limit: Int<br/>
   * &nbsp;&nbsp;offset: Int<br/>
   * &nbsp;&nbsp;order_by: [addresses_order_by]<br/>
   * &nbsp;&nbsp;where: addresses_bool_exp<br/>
   * ): [addresses]<br/>
   * addresses_by_pk(id: Int!): addresses // 实现主键查询<br/>
   * addresses_aggregate( // 实现聚合查询<br/>
   * &nbsp;&nbsp;distinct_on: [addresses_select_column!]<br/>
   * &nbsp;&nbsp;limit: Int<br/>
   * &nbsp;&nbsp;offset: Int<br/>
   * &nbsp;&nbsp;order_by: [addresses_order_by!]<br/>
   * &nbsp;&nbsp;where: addresses_bool_exp<br/>
   * ): addresses_aggregate!<br/>
   * </code>
   * <p>
   *
   * @param builders builders to which this schema will be applied
   */
  public void applyToGraphQLObjectTypes(List<GraphQLObjectType.Builder> builders) {
    Objects.requireNonNull(builders);

    graph.forEach(
        (tableName, fieldNameToGraphQLField) -> {
          GraphQLObjectType.Builder objectBuilder = GraphQLObjectType.newObject().name(tableName);

          Map<AggregateType, GraphQLObjectType.Builder> aggrBuilder = this.aggregateBuilder(tableName);

          fieldNameToGraphQLField.forEach(
              (fieldName, graphQLField) -> {
                String name = fieldName;
                GraphQLOutputType graphQLType = graphQLField.getGraphQLType();
                if (!ReferenceType.REFERENCING.equals(graphQLField.getReferenceType())
                    && !ReferenceType.REFERENCED.equals(graphQLField.getReferenceType())) {
                  name = graphQLField.getName().getKeyName();
                }

                GraphQLFieldDefinition.Builder fieldBuilder =
                    GraphQLFieldDefinition.newFieldDefinition()
                        .name(name)
                        .type(graphQLType);
                if (graphQLField.getReferenceType() == ReferenceType.REFERENCED) {
                  String foreignTableName = graphQLField.getForeignName().getTableName();
                  fieldBuilder
                      .argument(limit)
                      .argument(offset)
                      .argument(orderByMap.get(foreignTableName))
                      .argument(whereMap.get(foreignTableName));
                }
                objectBuilder.field(fieldBuilder.build());

                if (graphQLType == GraphQLInt ||
                    graphQLType == GraphQLFloat ||
                    graphQLType == GraphQLString) {
                  aggrBuilder.get(MAX).field(fieldBuilder.build());
                  aggrBuilder.get(MIN).field(fieldBuilder.build());

                  if (graphQLType != GraphQLString) {
                    aggrBuilder.get(AVG).field(fieldBuilder.build());
                    aggrBuilder.get(SUM).field(fieldBuilder.build());

                    aggrBuilder.get(STDDEV).field(fieldBuilder.type(GraphQLFloat).build());
                    aggrBuilder.get(STDDEV_POP).field(fieldBuilder.type(GraphQLFloat).build());
                    aggrBuilder.get(STDDEV_SAMP).field(fieldBuilder.type(GraphQLFloat).build());
                    aggrBuilder.get(VAR_POP).field(fieldBuilder.type(GraphQLFloat).build());
                    aggrBuilder.get(VAR_SAMP).field(fieldBuilder.type(GraphQLFloat).build());
                    aggrBuilder.get(VARIANCE).field(fieldBuilder.type(GraphQLFloat).build());
                  }
                }
              });

          GraphQLObjectType object = objectBuilder.build();

          GraphQLObjectType.Builder aggregateBuilder = GraphQLObjectType.newObject()
              .name(tableName + "_aggregate")
              .field(GraphQLFieldDefinition.newFieldDefinition()
                  .name("aggregate")
                  .type(this.aggregateFieldsBuilder(tableName, aggrBuilder).build())
              )
              .field(GraphQLFieldDefinition.newFieldDefinition()
                  .name("nodes")
                  .type(GraphQLList.list(object)));


          GraphQLObjectType aggregate = aggregateBuilder.build();

          builders.forEach(
              builder -> {
                builder.field(
                    GraphQLFieldDefinition.newFieldDefinition()
                        .name(tableName)
                        .type(GraphQLList.list(object))
                        .argument(limit)
                        .argument(offset)
                        .argument(distinctOnMap.get(tableName))
                        .argument(orderByMap.get(tableName))
                        .argument(whereMap.get(tableName))
                        .build());

                builder.field(
                    GraphQLFieldDefinition.newFieldDefinition()
                        .name(tableName + "_count")
                        .type(GraphQLInt)
                        .argument(limit)
                        .argument(offset)
                        .argument(distinctOnMap.get(tableName))
                        .argument(orderByMap.get(tableName))
                        .argument(whereMap.get(tableName))
                        .build());

                if (pkMap.containsKey(tableName)) {
                  builder.field(
                      GraphQLFieldDefinition.newFieldDefinition()
                          .name(tableName + "_by_pk")
                          .type(object)
                          .argument(pkMap.get(tableName))
                          .build());
                }

                // xx_aggregate
                builder.field(
                    GraphQLFieldDefinition.newFieldDefinition()
                        .name(tableName + "_aggregate")
                        .type(aggregate)
                        .argument(limit)
                        .argument(offset)
                        .argument(distinctOnMap.get(tableName))
                        .argument(orderByMap.get(tableName))
                        .argument(whereMap.get(tableName))
                        .build());
              }
          );
        });
  }

  /**
   * 构建参数
   *
   * @param name
   * @param type
   * @return
   */
  private static GraphQLArgument createArgument(String name, GraphQLScalarType type) {
    return GraphQLArgument.newArgument().name(name).type(type).build();
  }

  /**
   * 构建图信息
   *
   * @param schema
   * @return
   */
  private static Map<String, Map<String, GraphQLField>> createGraph(Schema schema) {
    Objects.requireNonNull(schema);
    Map<String, Map<String, GraphQLField>> graph = new HashMap<>();
    schema
        .getGraph()
        .forEach(
            (tableName, keyNameToKeyDefinition) -> {
              graph.put(tableName, new HashMap<>());
              Map<String, GraphQLField> fieldNameToGraphQLFieldDefinition = graph.get(tableName);
              keyNameToKeyDefinition.forEach(
                  (keyName, keyDefinition) -> {
                    QualifiedName qualifiedName = keyDefinition.getQualifiedName();
                    QualifiedName name = keyDefinition.getName();
                    QualifiedName referencing = keyDefinition.getReferencing();
                    Set<QualifiedName> referencedBySet = keyDefinition.getReferencedBy();
                    fieldNameToGraphQLFieldDefinition.put(
                        qualifiedName.getKeyName(),
                        GraphQLField.createLeaf(
                            qualifiedName,
                            name,
                            keyDefinition.getKeyType()
                        ));
                    fieldNameToGraphQLFieldDefinition.put(
                        name.getKeyName(),
                        GraphQLField.createLeaf(
                            qualifiedName,
                            name,
                            keyDefinition.getKeyType()
                        ));
                    if (referencing != null) {
                      // xx_ref
                      String referencingName =
                          GraphQLNaming.getNameForReferencingField(name);
                      fieldNameToGraphQLFieldDefinition.put(
                          referencingName,
                          GraphQLField.createReferencing(qualifiedName, name, referencing));
                    }
                    referencedBySet.forEach(
                        referencedBy -> {
                          // xx_on_xx
                          String referencedByName =
                              GraphQLNaming.getNameForReferencedByField(referencedBy);
                          fieldNameToGraphQLFieldDefinition.put(
                              referencedByName,
                              GraphQLField.createReferencedBy(qualifiedName, name, referencedBy));
                        });

                  });
            });
    return graph;
  }

  /**
   * 构建排序查询参数
   *
   * @param schema
   * @return
   */
  private static Map<String, GraphQLArgument> createDistinctOnMap(Schema schema) {
    Map<String, GraphQLArgument> distinctOnMap = new HashMap<>();
    schema
        .getGraph()
        .forEach(
            (parent, subGraph) -> {
              String distinctOnName = GraphQLNaming.getNameDistinctOnType(parent);

              Set<String> distinctOns = subGraph.keySet();

              GraphQLEnumType enumType = DistinctOn.distinctOn(parent, distinctOnName, distinctOns);

              GraphQLInputType distinctOnType = GraphQLList.list(enumType);
              // create argument
              GraphQLArgument distinctOn =
                  GraphQLArgument.newArgument().name("distinct_on").type(distinctOnType).build();
              distinctOnMap.put(parent, distinctOn);
            });
    return distinctOnMap;
  }

  /**
   * 构建排序查询参数
   *
   * @param schema
   * @return
   */
  private static Map<String, GraphQLArgument> createOrderByMap(Schema schema) {
    Map<String, GraphQLArgument> orderByMap = new HashMap<>();
    schema
        .getGraph()
        .forEach(
            (parent, subGraph) -> {
              String orderByName = GraphQLNaming.getNameOrderByType(parent);
              GraphQLInputObjectType.Builder builder =
                  GraphQLInputObjectType.newInputObject().name(orderByName);
              subGraph.forEach(
                  (name, node) -> {
                    builder.field(
                        GraphQLInputObjectField.newInputObjectField()
                            .name(name)
                            .type(OrderBy.enumType)
                            .build());
                    // if node is referencing, add schema field referencing to corresponding schema
                    // type
                    if (node.getReferencing() != null) {
                      String referencingName =
                          GraphQLNaming.getNameForReferencingField(node.getQualifiedName());
                      String referencingTypeName =
                          GraphQLNaming.getNameOrderByType(node.getReferencing().getTableName());
                      builder.field(
                          GraphQLInputObjectField.newInputObjectField()
                              .name(referencingName)
                              .type(GraphQLTypeReference.typeRef(referencingTypeName))
                              .build());
                    }
                  });
              GraphQLInputType orderByType = GraphQLList.list(builder.build());
              // create argument
              GraphQLArgument orderBy =
                  GraphQLArgument.newArgument().name("order_by").type(orderByType).build();
              orderByMap.put(parent, orderBy);
            });
    return orderByMap;
  }

  private static Map<String, GraphQLArgument> createPKMap(Schema schema) {
    Map<String, GraphQLArgument> pkMap = new HashMap<>();
    schema
        .getGraph()
        .forEach(
            (parent, subGraph) -> {
              // 再构建字段查询参数
              subGraph.forEach(
                  (name, node) -> {
                    if (node.isPrimaryKey()) {
                      GraphQLInputType nodeType = GraphQLField.keyTypeToGraphQLType.get(node.getKeyType());
                      GraphQLArgument id =
                          GraphQLArgument.newArgument().name("id").type(nodeType).build();
                      pkMap.put(parent, id);
                    }
                  });

            });

    return pkMap;
  }

  /**
   * 构建where查询参数
   *
   * @param schema
   * @return
   */
  private static Map<String, GraphQLArgument> createWhereMap(Schema schema) {
    Map<String, GraphQLArgument> whereMap = new HashMap<>();
    schema
        .getGraph()
        .forEach(
            (parent, subGraph) -> {
              String whereName = GraphQLNaming.getNameBoolType(parent);
              GraphQLInputObjectType.Builder builder = GraphQLInputObjectType.newInputObject();
              // 先构建_and _not _or三个逻辑操作
              builder
                  .name(whereName)
                  .description(
                      "Boolean expression to filter rows from the table \""
                          + parent
                          + "\". All fields are combined with a logical 'AND'.")
                  .field(
                      GraphQLInputObjectField.newInputObjectField()
                          .name("_and")
                          .type(GraphQLList.list(GraphQLTypeReference.typeRef(whereName)))
                          .build())
                  .field(
                      GraphQLInputObjectField.newInputObjectField()
                          .name("_not")
                          .type(GraphQLTypeReference.typeRef(whereName))
                          .build())
                  .field(
                      GraphQLInputObjectField.newInputObjectField()
                          .name("_or")
                          .type(GraphQLList.list(GraphQLTypeReference.typeRef(whereName)))
                          .build());
              // 再构建字段查询参数
              subGraph.forEach(
                  (name, node) -> {
                    GraphQLInputType nodeType =
                        ConditionalOperatorTypes.scalarTypeToComparisonExpMap.get(
                            GraphQLField.keyTypeToGraphQLType.get(node.getKeyType()));
                    builder.field(
                        GraphQLInputObjectField.newInputObjectField()
                            .name(name)
                            .type(nodeType)
                            .build());
                    // 如果存在外键，则构建外键引用表的查询参数
                    if (node.getReferencing() != null) {
                      String referencingName =
                          GraphQLNaming.getNameForReferencingField(node.getName());
                      String referencingTypeName =
                          GraphQLNaming.getNameBoolType(node.getReferencing().getTableName());
                      builder.field(
                          GraphQLInputObjectField.newInputObjectField()
                              .name(referencingName)
                              .type(GraphQLTypeReference.typeRef(referencingTypeName))
                              .build());
                    } else if (node.getReferencedBy() != null) {
                      // 如果有外键引用，则构建引用对象的信息查询参数
                      for (QualifiedName qualifiedName : node.getReferencedBy()) {
                        String referencedByName =
                            GraphQLNaming.getNameForReferencedByField(qualifiedName);
                        String referencedByTypeName =
                            GraphQLNaming.getNameBoolType(qualifiedName.getTableName());
                        builder.field(
                            GraphQLInputObjectField.newInputObjectField()
                                .name(referencedByName)
                                .type(GraphQLTypeReference.typeRef(referencedByTypeName))
                                .build());
                      }
                    }
                  });
              GraphQLInputType whereType = builder.build();
              GraphQLArgument where =
                  GraphQLArgument.newArgument().name("where").type(whereType).build();
              whereMap.put(parent, where);
            });
    return whereMap;
  }
}
