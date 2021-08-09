package com.wenj91.fastgql.core.sql;


import com.wenj91.fastgql.common.enums.LogicalConnective;
import com.wenj91.fastgql.common.enums.RelationalOperator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * 查询条件数据结构
 */
public class Condition {

  /**
   * 外键信息
   */
  static class Referencing {
    /**
     * 外键引用表
     */
    private final String foreignTable;
    /**
     * 外键引用表的列
     */
    private final String foreignColumn;
    /**
     * 外键列
     */
    private final String column;
    /**
     * 查询条件
     */
    private final Condition condition;

    public Referencing(
        String foreignTable, String foreignColumn, String column, Condition condition) {
      this.foreignTable = foreignTable;
      this.foreignColumn = foreignColumn;
      this.column = column;
      this.condition = condition;
    }

    public String getForeignTable() {
      return foreignTable;
    }

    public String getColumn() {
      return column;
    }

    public String getForeignColumn() {
      return foreignColumn;
    }

    public Condition getCondition() {
      return condition;
    }

    @Override
    public String toString() {
      return "Referencing<"
          + "foreignTable: '"
          + foreignTable
          + '\''
          + ", foreignColumn: '"
          + foreignColumn
          + '\''
          + ", column: '"
          + column
          + '\''
          + ", condition: "
          + condition
          + '>';
    }
  }

  /**
   * 名称
   */
  private String column;
  /**
   * 操作
   */
  private RelationalOperator operator;
  /**
   * 函数
   */
  private Function<Map<String, Object>, Object> function;
  private LogicalConnective connective;
  /**
   * 外键信息
   */
  private Referencing referencing;
  /**
   * 是否为 否定操作
   */
  private boolean negated;
  /**
   * 子查询条件
   */
  private final List<Condition> next = new ArrayList<>();

  public Condition() {
    this.negated = false;
  }

  public Condition(
      String column, RelationalOperator operator, Function<Map<String, Object>, Object> function) {
    this.column = column;
    this.operator = operator;
    this.function = function;
    this.negated = false;
  }

  public static Condition createReferencing(
      String foreignTable, String foreignColumn, String column, Condition condition) {
    Referencing referencing = new Referencing(foreignTable, foreignColumn, column, condition);
    Condition retCondition = new Condition();
    retCondition.setReferencing(referencing);
    return retCondition;
  }

  public String getColumn() {
    return column;
  }

  public RelationalOperator getOperator() {
    return operator;
  }

  public Function<Map<String, Object>, Object> getFunction() {
    return function;
  }

  public LogicalConnective getConnective() {
    return connective;
  }

  public boolean isNegated() {
    return negated;
  }

  public List<Condition> getNext() {
    return next;
  }

  public void setColumn(String column) {
    this.column = column;
  }

  public void setOperator(RelationalOperator operator) {
    this.operator = operator;
  }

  public void setFunction(Function<Map<String, Object>, Object> function) {
    this.function = function;
  }

  public void setConnective(LogicalConnective connective) {
    this.connective = connective;
  }

  public void setNegated(boolean negated) {
    this.negated = negated;
  }

  public Referencing getReferencing() {
    return referencing;
  }

  public void setReferencing(Referencing referencing) {
    this.referencing = referencing;
  }

  @Override
  public String toString() {
    return String.format(
        "Condition<referencing: %s, negated: %s, column: %s, operator: %s, function: %s, connective: %s, next: %s>",
        referencing, negated, column, operator, function, connective, next);
  }
}
