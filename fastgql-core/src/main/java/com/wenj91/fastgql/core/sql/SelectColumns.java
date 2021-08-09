package com.wenj91.fastgql.core.sql;

import com.wenj91.fastgql.common.enums.AggregateType;
import lombok.Getter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
public class SelectColumns {

  private final Map<AggregateType, List<SelectColumn>> aggregateTypeListMap = new HashMap<>();

  private final List<SelectColumn> select = new ArrayList<>();
  private final Map<String, SelectColumns> referencing = new HashMap<>();
  private final Map<String, ReferencedColumn> referenced = new HashMap<>();

  SelectColumns() {
    for (AggregateType type : AggregateType.values()) {
      aggregateTypeListMap.put(type, new ArrayList<>());
    }
  }

  public void aggregate(AggregateType type, List<SelectColumn> selectColumns) {
    aggregateTypeListMap.get(type).addAll(selectColumns);
  }

  public List<SelectColumn> getAggregate(AggregateType type) {
    return this.aggregateTypeListMap.get(type);
  }

  public void select(SelectColumn selectColumn) {
    select.add(selectColumn);
  }

  public void selects(List<SelectColumn> selectColumns) {
    select.addAll(selectColumns);
  }

  public void referencing(String columnName, SelectColumns selectColumns) {
    referencing.put(columnName, selectColumns);
  }

  public void referencings(Map<String, SelectColumns> referencings) {
    referencing.putAll(referencings);
  }

  public void referenced(String columnName, ReferencedColumn referencedColumn) {
    referenced.put(columnName, referencedColumn);
  }

  public void referenceds(Map<String, ReferencedColumn> referencedColumns) {
    referenced.putAll(referencedColumns);
  }
}
