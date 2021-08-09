package com.wenj91.fastgql.core.sql;

import com.wenj91.fastgql.core.graphql.FastGraphQLSchema;
import graphql.language.Argument;
import graphql.language.IntValue;

import java.math.BigInteger;
import java.util.List;

public class Arguments {

  private static final String DISTINCT_ON = "distinct_on";
  private static final String WHERE = "where";
  private static final String ORDER_BY = "order_by";
  private static final String LIMIT = "limit";
  private static final String OFFSET = "offset";
  private static final String ID = "id";

  private final Condition condition;
  private final List<OrderBy> orderByList;
  private final List<DistinctOn> distinctOnList;
  private final BigInteger limit;
  private final BigInteger offset;

  public Arguments() {
    this.condition = null;
    this.orderByList = null;
    this.distinctOnList = null;
    this.limit = null;
    this.offset = null;
  }

  public Arguments(
      List<Argument> arguments,
      String tableName,
      String tableAlias,
      FastGraphQLSchema fastGraphQLSchema) {
    Condition condition = null;
    List<OrderBy> orderByList = null;
    List<DistinctOn> distinctOnList = null;
    BigInteger limit = null;
    BigInteger offset = null;

    for (Argument argument : arguments) {
      switch (argument.getName()) {
        case WHERE:
          condition = ConditionUtils.createCondition(argument, tableName, fastGraphQLSchema);
          break;
        case ORDER_BY:
          orderByList =
              OrderByUtils.createOrderBy(argument, tableName, tableAlias, fastGraphQLSchema);
          break;
        case LIMIT:
          limit = ((IntValue) argument.getValue()).getValue();
          break;
        case OFFSET:
          offset = ((IntValue) argument.getValue()).getValue();
          break;
        case ID:
          condition = ConditionUtils.createIntCondition(argument);
          break;
        case DISTINCT_ON:
          distinctOnList = DistinctOnUtils.createDistinctOn(argument, tableName, tableAlias, fastGraphQLSchema);
          break;
        default:
          break;
      }
    }
    this.condition = condition;
    this.orderByList = orderByList;
    this.limit = limit;
    this.offset = offset;
    this.distinctOnList = distinctOnList;
  }

  public Condition getCondition() {
    return condition;
  }

  public List<OrderBy> getOrderByList() {
    return orderByList;
  }

  public List<DistinctOn> getDistinctOn() {
    return distinctOnList;
  }

  public BigInteger getLimit() {
    return limit;
  }

  public BigInteger getOffset() {
    return offset;
  }
}
