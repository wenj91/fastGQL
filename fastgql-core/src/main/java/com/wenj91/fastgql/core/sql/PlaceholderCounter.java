package com.wenj91.fastgql.core.sql;


import com.wenj91.fastgql.common.enums.DBType;

import java.util.concurrent.atomic.AtomicInteger;

public class PlaceholderCounter {

  private final AtomicInteger atomicInteger = new AtomicInteger(1);
  private final DBType dbType;

  PlaceholderCounter(DBType dbType) {
    this.dbType = dbType;
  }

  String next() {
    switch (dbType) {
      case postgresql:
        return String.format("$%d", atomicInteger.getAndIncrement());
      case mysql:
        return "?";
      default:
        throw new RuntimeException("DB type not supported");
    }
  }
}
