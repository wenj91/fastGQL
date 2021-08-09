package com.wenj91.fastgql.executor;

import lombok.Data;

@Data
public class Result {
  private Long lastInsertId;
  private Integer rowsAffected;

  public static Result of(Long lastInsertId, Integer rowsAffected) {
    Result result = new Result();
    result.lastInsertId = lastInsertId;
    result.rowsAffected = rowsAffected;

    return result;
  }
}
