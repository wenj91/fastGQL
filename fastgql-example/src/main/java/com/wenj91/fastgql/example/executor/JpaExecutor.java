package com.wenj91.fastgql.example.executor;

import com.wenj91.fastgql.executor.Executor;
import com.wenj91.fastgql.executor.Result;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

@Component
public class JpaExecutor implements Executor {

  @Resource
  private JdbcTemplate jdbcTemplate;

  @Override
  public List<Map<String, Object>> query(String sql, Object... args) {
    return jdbcTemplate.queryForList(sql, args);
  }

  @Override
  public Integer count(String sql, Object... args) {
    return jdbcTemplate.queryForObject(sql.toString(), Integer.class, args);
  }

  @Override
  public Result exec(String sql, Object... args) {
    Integer affected = jdbcTemplate.update(sql, args);

    return Result.of(0L, affected);
  }
}
