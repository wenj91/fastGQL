package com.wenj91.fastgql.core.spqr;

import com.wenj91.fastgql.annotation.Permissions;
import com.wenj91.fastgql.annotation.Roles;
import com.wenj91.fastgql.core.interceptor.DataFetcherInterceptor;
import com.wenj91.fastgql.core.interceptor.DataFetcherInterceptorHandler;
import io.leangen.graphql.execution.InvocationContext;
import io.leangen.graphql.execution.ResolverInterceptor;

import java.lang.reflect.AnnotatedElement;
import java.util.Collections;
import java.util.List;

public class SpqrInterceptor implements ResolverInterceptor {
  private DataFetcherInterceptorHandler dataFetcherInterceptorHandler;

  public SpqrInterceptor(List<DataFetcherInterceptor> interceptors) {
    this.dataFetcherInterceptorHandler = new DataFetcherInterceptorHandler(interceptors);
  }

  @Override
  public Object aroundInvoke(InvocationContext context, Continuation continuation) throws Exception {
    AnnotatedElement delegate = context.getResolver().getExecutable().getDelegate();
    Object ctx = context.getResolutionEnvironment().dataFetchingEnvironment.getContext();

    Permissions permissions = delegate.getAnnotation(Permissions.class);
    Roles roles = delegate.getAnnotation(Roles.class);

    boolean handleResult = dataFetcherInterceptorHandler.perHandle(
        ctx,
        Collections.singleton(roles),
        Collections.singleton(permissions)
    );
    if (!handleResult) {
      throw new RuntimeException("The result of Interceptor is false");
    }

    Object res = null;
    Exception exception = null;
    try {
      res = continuation.proceed(context);
    } catch (Exception e) {
      exception = e;
    } finally {
      dataFetcherInterceptorHandler.afterHandle(ctx, exception);
    }

    return res;
  }
}
