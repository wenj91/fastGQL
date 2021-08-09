package com.wenj91.fastgql.core.interceptor;

import com.wenj91.fastgql.annotation.Permissions;
import com.wenj91.fastgql.annotation.Roles;

import java.util.Set;

public interface DataFetcherInterceptor {
  boolean preHandle(Object context, Set<Roles> roles, Set<Permissions> permissions) throws Exception;
  void afterCompletion(Object context, Exception ex) throws Exception;
}
