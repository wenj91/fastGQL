package com.wenj91.fastgql.common.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import java.util.HashMap;
import java.util.Map;


@Getter
@ToString
public enum AggregateType {
  MAX("max", "MAX", "%s_max_fields"),
  MIN("min", "MIN", "%s_min_fields"),
  SUM("sum", "SUM", "%s_sum_fields"),
  AVG("avg", "AVG", "%s_avg_fields"),
  STDDEV("stddev", "STDDEV", "%s_stddev_fields"),
  STDDEV_POP("stddev_pop", "STDDEV_POP", "%s_stddev_pop_fields"),
  STDDEV_SAMP("stddev_samp", "STDDEV_SAMP", "%s_stddev_samp_fields"),
  VAR_POP("var_pop", "VAR_POP", "%s_var_pop_fields"),
  VAR_SAMP("var_samp", "VAR_SAMP", "%s_var_samp_fields"),
  VARIANCE("variance", "VARIANCE", "%s_variance_fields"),
  ;

  String name;
  String func;
  String format;

  AggregateType(String name, String func, String format) {
    this.name = name;
    this.func = func;
    this.format = format;
  }

  static Map<String, AggregateType> mapper = new HashMap<>();

  static {
    for (AggregateType type : AggregateType.values()) {
      mapper.put(type.getName(), type);
    }
  }

  public static AggregateType getByName(String name) {
    return mapper.get(name);
  }
}
