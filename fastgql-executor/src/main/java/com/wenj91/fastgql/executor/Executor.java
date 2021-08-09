package com.wenj91.fastgql.executor;

import java.util.List;
import java.util.Map;

public interface Executor {
  List<Map<String, Object>> query(String sql, Object... args);
  Integer count(String sql, Object... args);
  Result exec(String sql, Object... args);
}
