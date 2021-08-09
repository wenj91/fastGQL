package com.wenj91.fastgql.core.interceptor;

import com.wenj91.fastgql.annotation.Permissions;
import com.wenj91.fastgql.annotation.Roles;

import java.util.List;
import java.util.Set;

public class DataFetcherInterceptorHandler {
  private List<DataFetcherInterceptor> interceptors;

  public DataFetcherInterceptorHandler(List<DataFetcherInterceptor> interceptors) {
    this.interceptors = interceptors;
  }

  public boolean perHandle(
      Object context,
      Set<Roles> roles,
      Set<Permissions> permissions
  ) throws Exception {
    if (this.interceptors != null) {
      for (DataFetcherInterceptor interceptor : this.interceptors) {
        boolean res = interceptor.preHandle(context, roles, permissions);
        if (!res) {
          return false;
        }
      }
    }

    return true;
  }

  public void afterHandle(Object context, Exception ex) throws Exception {
    if (this.interceptors != null) {
      for (DataFetcherInterceptor interceptor : this.interceptors) {
        interceptor.afterCompletion(context, ex);
      }
    }
  }
}
