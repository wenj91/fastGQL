/*
 * Copyright fastGQL Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package com.wenj91.fastgql.core.graphql;

import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLInputObjectType.Builder;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLScalarType;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

import static graphql.Scalars.*;

/**
 * 查询条件操作类型
 */
public class ConditionalOperatorTypes {

  // TODO: implement comparison expression for other GraphQL type
  public static final Map<GraphQLScalarType, GraphQLInputObjectType> scalarTypeToComparisonExpMap =
      Map.of(
          GraphQLInt, comparisonExpBuilder("Int", GraphQLInt).build(), // 通用类型：int
          GraphQLBoolean, comparisonExpBuilder("Boolean", GraphQLBoolean).build(), // 通用类型：bool
          GraphQLFloat, comparisonExpBuilder("Float", GraphQLFloat).build(), // 通用类型：float
          GraphQLString, stringComparisonExpBuilder().build()); // 字符串类型
  private static final Map<String, String> operatorNameToValueMap; // 操作字典

  static {
    operatorNameToValueMap = new HashMap<>();
    for (GenericOps op : GenericOps.values()) {
      operatorNameToValueMap.put(op.name, op.value);
    }
    for (TextOps op : TextOps.values()) {
      operatorNameToValueMap.put(op.name, op.value);
    }
  }

  public static Map<String, String> getOperatorNameToValueMap() {
    return operatorNameToValueMap;
  }

  private static Builder comparisonExpBuilder(String prefix, GraphQLScalarType type) {
    Builder builder = GraphQLInputObjectType.newInputObject().name(prefix + "_comparison_exp");
    addGenericOperators(builder, type);
    return builder;
  }

  /**
   * 字符串类型操作比较表达式构建
   *
   * @return
   */
  private static Builder stringComparisonExpBuilder() {
    Builder builder = GraphQLInputObjectType.newInputObject().name("String_comparison_exp");
    addGenericOperators(builder, GraphQLString); // 添加通用的
    addTextOperators(builder, GraphQLString); // 还要添加额外字符串专门操作的
    return builder;
  }

  /**
   * 通用类型操作构建
   *
   * @param builder
   * @param type
   */
  private static void addGenericOperators(Builder builder, GraphQLScalarType type) {
    for (GenericOps op : GenericOps.values()) {
      switch (op) {
        case _is_null:
          builder.field(
              GraphQLInputObjectField.newInputObjectField()
                  .name(op.name())
                  .type(GraphQLBoolean)
                  .build());
          break;
        case _in:
        case _nin:
          builder.field(
              GraphQLInputObjectField.newInputObjectField()
                  .name(op.name())
                  .type(GraphQLList.list(type))
                  .build());
          break;
        default:
          builder.field(
              GraphQLInputObjectField.newInputObjectField().name(op.name()).type(type).build());
      }
    }
  }

  /**
   * 字符串类型操作构建
   *
   * @param builder
   * @param type
   */
  private static void addTextOperators(Builder builder, GraphQLScalarType type) {
    for (TextOps op : TextOps.values()) {
      builder.field(
          GraphQLInputObjectField.newInputObjectField().name(op.name()).type(type).build());
    }
  }

  // TODO: implement Type casting, JSONB operators, PostGIS related operators on GEOMETRY columns
  // https://hasura.io/docs/1.0/graphql/manual/api-reference/graphql-api/query.html#operator
  @Getter
  public enum GenericOps {
    _eq("_eq", "=", "equal"),
    _neq("_neq", "<>", "not equal"),
    _in("_in", "IN", "in"),
    _nin("_nin", "NOT IN", "not in"),
    _gt("_gt", ">", "greater than"),
    _lt("_lt", "<", "less than"),
    _gte("_gte", ">=", "greater than or equal"),
    _lte("_lte", "<=", "less than or equal"),
    _is_null("_is_null", "IS NULL", "is null");

    private final String name;
    private final String value;
    private final String description;

    GenericOps(String name, String value, String description) {
      this.name = name;
      this.value = value;
      this.description = description;
    }
  }

  @Getter
  public enum TextOps {
    _like("_like", "LIKE", "like"),
    _nlike("_nlike", "NOT LIKE", "not like"),
    _ilike("_ilike", "ILIKE", "case insensitive like"),
    _nilike("_nilike", "NOT ILIKE", "case insensitive not like"),
    _similar("_similar", "SIMILAR TO", "similar to"),
    _nsimilar("_nsimilar", "NOT SIMILAR TO", "not similar to");

    private final String name;
    private final String value;
    private final String description;

    TextOps(String name, String value, String description) {
      this.name = name;
      this.value = value;
      this.description = description;
    }
  }
}
