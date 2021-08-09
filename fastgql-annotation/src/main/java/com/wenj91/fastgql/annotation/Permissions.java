package com.wenj91.fastgql.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.FIELD})
public @interface Permissions {
  /**
   * 权限拦截method name
   * @return
   */
  String name();

  /**
   * 权限操作名称
   * @return
   */
  String[] value();

  /**
   * 权限操作逻辑符
   * @return
   */
  Logical logical() default Logical.AND;
}
