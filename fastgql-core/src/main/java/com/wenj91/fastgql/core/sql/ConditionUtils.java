package com.wenj91.fastgql.core.sql;

import com.wenj91.fastgql.common.enums.LogicalConnective;
import com.wenj91.fastgql.common.enums.RelationalOperator;
import com.wenj91.fastgql.core.graphql.FastGraphQLSchema;
import com.wenj91.fastgql.core.graphql.GraphQLField;
import com.wenj91.fastgql.core.schema.TableInfo;
import com.wenj91.fastgql.core.util.StringUtil;
import graphql.language.*;

import java.util.*;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

public class ConditionUtils {
  static Condition checkColumnIsEqValue(String columnName, Object value) {
    return new Condition(columnName, RelationalOperator._eq, params -> value);
  }

  public static Set<TableAlias> conditionToTableAliasSet(Condition condition, String tableAlias) {
    Set<TableAlias> ret =
        condition.getNext().stream()
            .map(it -> conditionToTableAliasSet(it, tableAlias))
            .flatMap(Collection::stream)
            .collect(Collectors.toSet());

    Condition.Referencing referencing = condition.getReferencing();

    if (referencing != null) {
      String table = referencing.getForeignTable();
      String alias = String.format("%sr", tableAlias);
      ret.add(new TableAlias(table, alias));
      ret.addAll(conditionToTableAliasSet(referencing.getCondition(), alias));
    }
    return ret;
  }

  /**
   * 构建查询条件
   *
   * @param condition
   * @param tableAlias
   * @param jwtParams
   * @return
   */
  private static PreparedQuery conditionToSQLInternal(
      Condition condition, String tableAlias, Map<String, Object> jwtParams, FastGraphQLSchema schema) {

    // 如果存在多个条件，先处理其他条件
    PreparedQuery nextPreparedQuery =
        condition.getNext().stream()
            .map(it -> conditionToSQLInternal(it, tableAlias, jwtParams, schema))
            .collect(PreparedQuery.collector());

    // sql逻辑操作符生成
    PreparedQuery connective =
        condition.getConnective() != null
            ? PreparedQuery.create(condition.getConnective().toString().toUpperCase()).merge(" ")
            : PreparedQuery.create();

    // 处理其他条件生成的query
    PreparedQuery nextPreparedQueryWithSpace =
        !nextPreparedQuery.isEmpty()
            ? PreparedQuery.create(" ").merge(nextPreparedQuery)
            : nextPreparedQuery;

    // 外键信息
    Condition.Referencing referencing = condition.getReferencing();

    // 是否负操作
    PreparedQuery not =
        condition.isNegated() ? PreparedQuery.create("NOT ") : PreparedQuery.create();

    // query组装
    PreparedQuery rootConditionPrepared = PreparedQuery.create();

    // 如果存在外键信息则先处理外键信息
    if (referencing != null) {
      String referencingAlias = String.format("%sr", tableAlias);

      String foreignTable = referencing.getForeignTable();
      TableInfo tableInfo = schema.tableInfo(foreignTable);

      rootConditionPrepared.merge(
          String.format(
              "EXISTS (SELECT 1 FROM %s %s WHERE %s.%s=%s.%s AND (",
              tableInfo.getTableName(),
              referencingAlias,
              referencingAlias,
              referencing.getForeignColumn(),
              tableAlias,
              referencing.getColumn()));
      rootConditionPrepared.merge(
          conditionToSQLInternal(referencing.getCondition(), referencingAlias, jwtParams, schema));
      rootConditionPrepared.merge("))");
    } else if (condition.getColumn() != null
        && condition.getOperator() != null
        && condition.getFunction() != null) { // 否则
      Object value = condition.getFunction().apply(jwtParams);

      RelationalOperator operator = condition.getOperator();

      if (!StringUtil.isBlank(tableAlias)) {
        rootConditionPrepared
            .merge(tableAlias)
            .merge(".");
      }

      rootConditionPrepared.merge(condition.getColumn())
          .merge(condition.getOperator().getSql());

      switch (operator) {
        case _in:
        case _nin:
          rootConditionPrepared.merge("(");
          if (value instanceof List) {
            List<?> valueList = (List<?>) value;
            for (int i = 0; i < valueList.size(); i++) {
              rootConditionPrepared.addParam(valueList.get(i));
              if (i < valueList.size() - 1) {
                rootConditionPrepared.merge(", ");
              }
            }
          } else {
            rootConditionPrepared.addParam(value);
          }
          rootConditionPrepared.merge(")");
          break;
        default:
          rootConditionPrepared.addParam(value);
      }
    }

    // 如果根节点不为空，存在负操作并且连接符不为空，则返回根节点与其他条件处理的合并信息
    if (!rootConditionPrepared.isEmpty()) {
      if (not.isEmpty() && connective.isEmpty()) {
        return rootConditionPrepared.merge(nextPreparedQueryWithSpace);
      }

      // 如果没有其他条件，则返回负操作与根节点的合并信息
      if (nextPreparedQueryWithSpace.isEmpty()) {
        return not.merge(connective).merge(rootConditionPrepared);
      }

      // 否则返回
      return not.merge(" ").merge(connective)
          .merge(PreparedQuery.create("("))
          .merge(rootConditionPrepared)
          .merge(nextPreparedQueryWithSpace)
          .merge(PreparedQuery.create(")"));
    }

    // 其它情况一律返回其它条件合并信息
    return connective.isEmpty() ? nextPreparedQuery : connective.merge(nextPreparedQuery);
  }

  static PreparedQuery conditionToSQL(
      Condition condition, String tableAlias, Map<String, Object> jwtParams, FastGraphQLSchema schema) {
    return conditionToSQLInternal(condition, tableAlias, jwtParams, schema);
  }

  private static String listToSql(List<?> list) {
    return String.format(
        "(%s)", list.stream().map(ConditionUtils::objectToSql).collect(Collectors.joining(", ")));
  }

  private static String objectToSql(Object object) {
    if (object instanceof String) {
      return String.format("'%s'", object);
    } else if (object instanceof List) {
      return listToSql((List<?>) object);
    } else if (object instanceof Object[]) {
      return listToSql(Arrays.asList((Object[]) object));
    } else {
      return String.valueOf(object);
    }
  }

  private static Object valueToObject(Value<?> value) {
    if (value instanceof IntValue) {
      return ((IntValue) value).getValue();
    } else if (value instanceof FloatValue) {
      return ((FloatValue) value).getValue();
    } else if (value instanceof StringValue) {
      return ((StringValue) value).getValue();
    } else if (value instanceof BooleanValue) {
      return ((BooleanValue)value).isValue() ? 1 : 0;
    } else if (value instanceof ArrayValue) {
      return ((ArrayValue) value)
          .getValues().stream().map(ConditionUtils::valueToObject).collect(Collectors.toList());
    } else {
      throw new RuntimeException("not recognized object type: " + value.getClass());
    }
  }

  private static BinaryOperator<Condition> conditionReducer(LogicalConnective logicalConnective) {
    return (Condition c1, Condition c2) -> {
      c2.setConnective(logicalConnective);
      c1.getNext().add(c2);
      return c1;
    };
  }

  // 构造基本条件数据
  private static Condition createBasicCondition(
      String name,
      ObjectValue objectValue,
      String pathInQuery,
      FastGraphQLSchema fastGraphQLSchema) {
    return objectValue.getObjectFields().stream()
        .map(
            objectField -> {
              GraphQLField graphQLField = fastGraphQLSchema.fieldAt(pathInQuery, name);
              Objects.requireNonNull(graphQLField);
              String column = graphQLField.getQualifiedName().getKeyName();
              if (List.of("_and", "_or", "_not").contains(objectField.getName())
                  || objectField.getValue() instanceof ObjectValue) {
                String foreignColumn = graphQLField.getForeignName().getKeyName();
                String foreignTable = graphQLField.getForeignName().getTableName();
                Condition innerConditionReferencing =
                    createConditionFromObjectField(
                        objectField, foreignTable, fastGraphQLSchema);
                return Condition.createReferencing(
                    foreignTable, foreignColumn, column, innerConditionReferencing);
              } else {
                return new Condition(
                    column,
                    RelationalOperator.valueOf(objectField.getName()),
                    params -> valueToObject(objectField.getValue()));
              }
            })
        .reduce(conditionReducer(LogicalConnective.and))
        .orElseThrow();
  }

  private static Condition createArrayCondition(
      ArrayValue arrayValue,
      LogicalConnective logicalConnective,
      String pathInQuery,
      FastGraphQLSchema fastGraphQLSchema) {
    return arrayValue.getValues().stream()
        .map(
            node ->
                createConditionFromObjectValue(
                    (ObjectValue) node, pathInQuery, fastGraphQLSchema))
        .reduce(conditionReducer(logicalConnective))
        .orElseThrow();
  }

  private static Condition createConditionFromObjectField(
      ObjectField objectField, String pathInQuery, FastGraphQLSchema fastGraphQLSchema) {
    switch (objectField.getName()) {
      case "_and":
        return objectField.getValue() instanceof ArrayValue
            ? createArrayCondition(
            (ArrayValue) objectField.getValue(),
            LogicalConnective.and,
            pathInQuery,
            fastGraphQLSchema)
            : createConditionFromObjectValue(
            (ObjectValue) objectField.getValue(), pathInQuery, fastGraphQLSchema);
      case "_or":
        return objectField.getValue() instanceof ArrayValue
            ? createArrayCondition(
            (ArrayValue) objectField.getValue(),
            LogicalConnective.or,
            pathInQuery,
            fastGraphQLSchema)
            : createConditionFromObjectValue(
            (ObjectValue) objectField.getValue(), pathInQuery, fastGraphQLSchema);
      case "_not":
        Condition notCondition = new Condition();
        Condition condition =
            createConditionFromObjectValue(
                (ObjectValue) objectField.getValue(), pathInQuery, fastGraphQLSchema);
        condition.setNegated(true);
        notCondition.getNext().add(condition);
        return notCondition;
      default:
        return createBasicCondition(
            objectField.getName(),
            (ObjectValue) objectField.getValue(),
            pathInQuery,
            fastGraphQLSchema);
    }
  }

  private static Condition createConditionFromObjectValue(
      ObjectValue objectValue, String pathInQuery, FastGraphQLSchema fastGraphQLSchema) {
    return objectValue.getObjectFields().stream()
        .map(
            objectField ->
                createConditionFromObjectField(objectField, pathInQuery, fastGraphQLSchema))
        .reduce(conditionReducer(LogicalConnective.and))
        .orElseThrow();
  }


  static Condition createCondition(
      Argument argument, String tableName, FastGraphQLSchema fastGraphQLSchema) {
    return createConditionFromObjectValue(
        (ObjectValue) argument.getValue(), tableName, fastGraphQLSchema);
  }

  static Condition createIntCondition(
      Argument argument) {
    return new Condition(
        argument.getName(),
        RelationalOperator._eq,
        params -> valueToObject(argument.getValue()));
  }
}
