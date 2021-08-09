package com.wenj91.fastgql.example.interceptor;

import com.wenj91.fastgql.annotation.Permissions;
import com.wenj91.fastgql.annotation.Roles;
import com.wenj91.fastgql.core.interceptor.DataFetcherInterceptor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Set;

@Slf4j
@Component
public class AuthorizerInterceptor implements DataFetcherInterceptor {

  @Override
  public boolean preHandle(Object context, Set<Roles> roles, Set<Permissions> permissions) throws Exception {
    log.info("AuthorizerInterceptor perHandle:{} -- {}", roles, permissions);
    return true;
  }

  @Override
  public void afterCompletion(Object context, Exception ex) throws Exception {
    log.info("AuthorizerInterceptor afterHandle");
    if (null != ex) {
      throw ex;
    }
  }
}
